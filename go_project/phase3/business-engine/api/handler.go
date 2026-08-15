package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"business-engine/broker"
	"business-engine/db"
)

// --- STRUCTS ---

type User struct {
	ID    int    `json:"id"`
	Email string `json:"email"`
	Name  string `json:"name"`
}

type BookingRequest struct {
	UserID    int    `json:"user_id"`
	RoomID    int    `json:"room_id"`
	StartDate string `json:"start_date"` // Format: YYYY-MM-DD
	EndDate   string `json:"end_date"`   // Format: YYYY-MM-DD
}

// --- ROUTER SETUP ---

// handlerEnv holds shared dependencies (DB pool access is via db.Pool globally,
// but the Redis broker is injected explicitly so handlers stay testable).
type handlerEnv struct {
	redis *broker.RedisClient
}

// NewRouter now takes the Redis broker so mutating handlers can enqueue
// background jobs instead of doing the work inline.
func NewRouter(redisBroker *broker.RedisClient) http.Handler {
	env := &handlerEnv{redis: redisBroker}
	mux := http.NewServeMux()

	// User Endpoints
	mux.HandleFunc("POST /users", env.createUser)
	mux.HandleFunc("PUT /users/{id}", env.updateUser)
	mux.HandleFunc("DELETE /users/{id}", env.deactivateUser)

	// Booking Endpoints
	mux.HandleFunc("POST /bookings", env.createBooking)
	mux.HandleFunc("PUT /bookings/{id}", env.updateBooking)
	mux.HandleFunc("POST /bookings/{id}/cancel", env.cancelBooking)

	// Availability Endpoint (read-only, left untouched)
	mux.HandleFunc("GET /rooms/available", checkAvailability)

	return mux
}

// --- ASYNC BACKGROUND JOBS ---
//
// Every mutating endpoint below used to fire three blocking steps before
// responding: a transactional email, an SMS, and an audit log write. Those
// are now serialized as TaskPayload messages and pushed onto a Redis list
// (queue name below) with LPUSH. A separate worker process is expected to
// BRPOP this queue and perform the actual email/SMS send + audit_logs
// insert out of band. This keeps the HTTP request/response cycle limited
// to the PostgreSQL transactional work (inserts, GiST range checks, etc).
//
// checkAvailability is intentionally left out: it's a read-only endpoint
// with no business event to notify about or audit.

const backgroundJobsQueue = "background_jobs"

// TaskPayload is the strict, typed message pushed onto the Redis queue for
// every background task. A single business event (e.g. "user_registered")
// fans out into multiple TaskPayload entries -- one per side effect (email,
// sms, audit_log) -- so a worker can process/retry each independently.
type TaskPayload struct {
	Type     string `json:"type"`      // e.g., "email", "sms", "audit_log"
	Entity   string `json:"entity"`    // e.g., "user" or "booking"
	EntityID int    `json:"entity_id"` // e.g., u.ID or bookingID
	Details  string `json:"details"`   // descriptive message
}

// enqueueTask serializes a TaskPayload into JSON and LPUSHes it onto the
// background jobs queue. Errors are returned to the caller so handlers can
// decide how to respond (see note in each handler about partial-failure
// semantics).
func (e *handlerEnv) enqueueTask(ctx context.Context, task TaskPayload) error {
	data, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("failed to marshal task payload: %w", err)
	}
	if err := e.redis.Client.LPush(ctx, backgroundJobsQueue, data).Err(); err != nil {
		return fmt.Errorf("failed to enqueue task: %w", err)
	}
	return nil
}

// enqueueEventTasks is a small helper that fans a single business event out
// into the three side-effect tasks (email, SMS, audit log) every handler
// below needs, serializes each as a TaskPayload, and pushes them all onto
// the queue.
func (e *handlerEnv) enqueueEventTasks(ctx context.Context, entity string, entityID int, details string) error {
	tasks := []TaskPayload{
		{Type: "email", Entity: entity, EntityID: entityID, Details: details},
		{Type: "sms", Entity: entity, EntityID: entityID, Details: details},
		{Type: "audit_log", Entity: entity, EntityID: entityID, Details: details},
	}
	for _, task := range tasks {
		if err := e.enqueueTask(ctx, task); err != nil {
			return err
		}
	}
	return nil
}

// --- USER HANDLERS ---

func (e *handlerEnv) createUser(w http.ResponseWriter, r *http.Request) {
	var u User
	if err := json.NewDecoder(r.Body).Decode(&u); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	query := `INSERT INTO users (email, name) VALUES ($1, $2) RETURNING id, email, name`
	err := db.Pool.QueryRow(r.Context(), query, u.Email, u.Name).Scan(&u.ID, &u.Email, &u.Name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// User row is committed at this point. Push notification + audit work
	// to the queue rather than blocking the response on it. If enqueueing
	// itself fails (Redis unreachable), we log it but still return 201 --
	// the user WAS created; a failed enqueue is an infra problem for a
	// worker/alerting layer to catch, not a reason to fail the request.
	details := fmt.Sprintf("User %s (id=%d) registered", u.Email, u.ID)
	if err := e.enqueueEventTasks(r.Context(), "user", u.ID, details); err != nil {
		fmt.Printf("warning: failed to enqueue jobs for user_registered (user_id=%d): %v\n", u.ID, err)
	}

	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(u)
}

func (e *handlerEnv) updateUser(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	id, err := strconv.Atoi(idStr)
	if err != nil {
		http.Error(w, "Invalid user ID", http.StatusBadRequest)
		return
	}

	var u User
	if err := json.NewDecoder(r.Body).Decode(&u); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	query := `UPDATE users SET name = $1, email = $2 WHERE id = $3 RETURNING id, email, name`
	err = db.Pool.QueryRow(r.Context(), query, u.Name, u.Email, id).Scan(&u.ID, &u.Email, &u.Name)
	if err != nil {
		http.Error(w, "User not found or update failed", http.StatusNotFound)
		return
	}

	details := fmt.Sprintf("User %s (id=%d) profile updated", u.Email, u.ID)
	if err := e.enqueueEventTasks(r.Context(), "user", u.ID, details); err != nil {
		fmt.Printf("warning: failed to enqueue jobs for user_updated (user_id=%d): %v\n", u.ID, err)
	}

	json.NewEncoder(w).Encode(u)
}

func (e *handlerEnv) deactivateUser(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	id, err := strconv.Atoi(idStr)
	if err != nil {
		http.Error(w, "Invalid user ID", http.StatusBadRequest)
		return
	}

	// Deleting or clearing user data depending on business logic requirements
	query := `DELETE FROM users WHERE id = $1`
	_, err = db.Pool.Exec(r.Context(), query, id)
	if err != nil {
		http.Error(w, "Failed to delete user", http.StatusInternalServerError)
		return
	}

	// The user row is already gone at this point, so the audit log job is
	// the only remaining record that this user ever existed and was
	// deactivated. Enqueueing failures are logged but non-fatal to the
	// response, same rationale as above.
	details := fmt.Sprintf("User id=%d deactivated", id)
	if err := e.enqueueEventTasks(r.Context(), "user", id, details); err != nil {
		fmt.Printf("warning: failed to enqueue jobs for user_deactivated (user_id=%d): %v\n", id, err)
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"message": "User deactivated successfully"})
}

// --- BOOKING HANDLERS ---

func (e *handlerEnv) createBooking(w http.ResponseWriter, r *http.Request) {
	var req BookingRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Format range for PostgreSQL TSRANGE: '[start, end)'
	dateRange := fmt.Sprintf("[%s, %s)", req.StartDate, req.EndDate)

	query := `
		INSERT INTO bookings (user_id, room_id, status, during) 
		VALUES ($1, $2, 'confirmed', $3::tsrange) 
		RETURNING id, status`

	var bookingID int
	var status string
	err := db.Pool.QueryRow(r.Context(), query, req.UserID, req.RoomID, dateRange).Scan(&bookingID, &status)
	if err != nil {
		http.Error(w, "Booking failed: room may be booked for these dates (GiST conflict)", http.StatusConflict)
		return
	}

	details := fmt.Sprintf("Booking id=%d created for user_id=%d, room_id=%d, range=%s",
		bookingID, req.UserID, req.RoomID, dateRange)
	if err := e.enqueueEventTasks(r.Context(), "booking", bookingID, details); err != nil {
		fmt.Printf("warning: failed to enqueue jobs for booking_created (booking_id=%d): %v\n", bookingID, err)
	}

	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]any{
		"booking_id": bookingID,
		"status":     status,
		"message":    "Booking confirmed safely with concurrency control",
	})
}

func (e *handlerEnv) updateBooking(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	id, err := strconv.Atoi(idStr)
	if err != nil {
		http.Error(w, "Invalid booking ID", http.StatusBadRequest)
		return
	}

	var req BookingRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	dateRange := fmt.Sprintf("[%s, %s)", req.StartDate, req.EndDate)

	query := `
		UPDATE bookings 
		SET room_id = $1, during = $2::tsrange 
		WHERE id = $3 AND status != 'cancelled' 
		RETURNING id, status`

	var updatedID int
	var status string
	err = db.Pool.QueryRow(r.Context(), query, req.RoomID, dateRange, id).Scan(&updatedID, &status)
	if err != nil {
		http.Error(w, "Update failed or booking range overlaps with another reservation", http.StatusConflict)
		return
	}

	details := fmt.Sprintf("Booking id=%d updated: room_id=%d, range=%s", updatedID, req.RoomID, dateRange)
	if err := e.enqueueEventTasks(r.Context(), "booking", updatedID, details); err != nil {
		fmt.Printf("warning: failed to enqueue jobs for booking_updated (booking_id=%d): %v\n", updatedID, err)
	}

	json.NewEncoder(w).Encode(map[string]any{
		"booking_id": updatedID,
		"status":     status,
		"message":    "Booking updated successfully",
	})
}

func (e *handlerEnv) cancelBooking(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	id, err := strconv.Atoi(idStr)
	if err != nil {
		http.Error(w, "Invalid booking ID", http.StatusBadRequest)
		return
	}

	// Setting status to cancelled frees up the TSRANGE via the partial GiST index
	query := `UPDATE bookings SET status = 'cancelled' WHERE id = $1`
	_, err = db.Pool.Exec(r.Context(), query, id)
	if err != nil {
		http.Error(w, "Failed to cancel booking", http.StatusInternalServerError)
		return
	}

	details := fmt.Sprintf("Booking id=%d cancelled", id)
	if err := e.enqueueEventTasks(r.Context(), "booking", id, details); err != nil {
		fmt.Printf("warning: failed to enqueue jobs for booking_cancelled (booking_id=%d): %v\n", id, err)
	}

	json.NewEncoder(w).Encode(map[string]string{"message": "Booking cancelled successfully"})
}

// --- AVAILABILITY HANDLER (untouched, read-only) ---

func checkAvailability(w http.ResponseWriter, r *http.Request) {
	startDate := r.URL.Query().Get("start")
	endDate := r.URL.Query().Get("end")

	if startDate == "" || endDate == "" {
		http.Error(w, "Missing 'start' or 'end' query parameters", http.StatusBadRequest)
		return
	}

	query := `
		SELECT id, room_number, room_type, price_per_night 
		FROM rooms 
		WHERE id NOT IN (
			SELECT room_id FROM bookings 
			WHERE status != 'cancelled' 
			AND during && tsrange($1, $2)
		)`

	rows, err := db.Pool.Query(context.Background(), query, startDate, endDate)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	type Room struct {
		ID            int     `json:"id"`
		RoomNumber    string  `json:"room_number"`
		RoomType      string  `json:"room_type"`
		PricePerNight float64 `json:"price_per_night"`
	}

	var availableRooms []Room
	for rows.Next() {
		var room Room
		if err := rows.Scan(&room.ID, &room.RoomNumber, &room.RoomType, &room.PricePerNight); err != nil {
			continue
		}
		availableRooms = append(availableRooms, room)
	}

	json.NewEncoder(w).Encode(availableRooms)
}