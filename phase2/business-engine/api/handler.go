package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

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

func NewRouter() http.Handler {
	mux := http.NewServeMux()

	// User Endpoints
	mux.HandleFunc("POST /users", createUser)
	mux.HandleFunc("PUT /users/{id}", updateUser)
	mux.HandleFunc("DELETE /users/{id}", deactivateUser)

	// Booking Endpoints
	mux.HandleFunc("POST /bookings", createBooking)
	mux.HandleFunc("PUT /bookings/{id}", updateBooking)
	mux.HandleFunc("POST /bookings/{id}/cancel", cancelBooking)

	// Availability Endpoint
	mux.HandleFunc("GET /rooms/available", checkAvailability)

	return mux
}

// --- SIMULATED SIDE-EFFECTS ---
//
// Every mutating endpoint below fires the same three blocking steps before
// responding, standing in for the synchronous notification/audit work a
// "phase 1" monolith typically does inline: a transactional email, an SMS,
// and an audit log write. This is the deliberately naive baseline to be
// compared against a later phase where these move onto a queue (Redis,
// SQS, etc.) and stop blocking the request/response cycle.
//
// checkAvailability is intentionally left out: it's a read-only endpoint
// with no business event to notify about or audit.

const (
	simulatedEmailDelay = 100 * time.Millisecond // stand-in for an SMTP handshake + send
	simulatedSMSDelay   = 150 * time.Millisecond  // stand-in for a synchronous SMS gateway call (e.g. Twilio)
)

// writeAuditLog performs the synchronous audit DB write shared by every
// handler. entityType/entityID identify what the event happened to (e.g.
// "user"/u.ID or "booking"/bookingID) since not every handler has a user ID
// on hand (cancelBooking only ever sees a booking ID).
func writeAuditLog(ctx context.Context, entityType string, entityID int, event, details string) error {
	query := `INSERT INTO audit_logs (entity_type, entity_id, event, details, created_at) VALUES ($1, $2, $3, $4, $5)`
	_, err := db.Pool.Exec(ctx, query, entityType, entityID, event, details, time.Now())
	return err
}

// --- USER HANDLERS ---

func createUser(w http.ResponseWriter, r *http.Request) {
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

	// Simulated welcome email.
	time.Sleep(simulatedEmailDelay)
	// Simulated welcome SMS.
	time.Sleep(simulatedSMSDelay)

	// Audit log write. The user row already exists at this point (creation
	// succeeded), so a failure here is a partial-failure state, not a
	// creation failure. Reported as 500 for now to keep this baseline
	// loud; a real system would more likely still return 201 and push the
	// failed write to a retry/dead-letter queue.
	details := fmt.Sprintf("User %s (id=%d) registered", u.Email, u.ID)
	if err := writeAuditLog(r.Context(), "user", u.ID, "user_registered", details); err != nil {
		http.Error(w, fmt.Sprintf("user created but audit log failed: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(u)
}

func updateUser(w http.ResponseWriter, r *http.Request) {
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

	// Simulated "profile updated" email.
	time.Sleep(simulatedEmailDelay)
	// Simulated "profile updated" SMS.
	time.Sleep(simulatedSMSDelay)

	details := fmt.Sprintf("User %s (id=%d) profile updated", u.Email, u.ID)
	if err := writeAuditLog(r.Context(), "user", u.ID, "user_updated", details); err != nil {
		http.Error(w, fmt.Sprintf("user updated but audit log failed: %v", err), http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(u)
}

func deactivateUser(w http.ResponseWriter, r *http.Request) {
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

	// Simulated deactivation-confirmation email.
	time.Sleep(simulatedEmailDelay)
	// Simulated deactivation-confirmation SMS.
	time.Sleep(simulatedSMSDelay)

	// Note: the user row is already gone at this point, so the audit log
	// is the only remaining record that this user ever existed and was
	// deactivated. If this write fails, we still tell the client the
	// deactivation succeeded (it did) but flag the gap.
	details := fmt.Sprintf("User id=%d deactivated", id)
	if err := writeAuditLog(r.Context(), "user", id, "user_deactivated", details); err != nil {
		http.Error(w, fmt.Sprintf("user deactivated but audit log failed: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"message": "User deactivated successfully"})
}

// --- BOOKING HANDLERS ---

func createBooking(w http.ResponseWriter, r *http.Request) {
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

	// Simulated booking-confirmation email.
	time.Sleep(simulatedEmailDelay)
	// Simulated booking-confirmation SMS.
	time.Sleep(simulatedSMSDelay)

	details := fmt.Sprintf("Booking id=%d created for user_id=%d, room_id=%d, range=%s",
		bookingID, req.UserID, req.RoomID, dateRange)
	if err := writeAuditLog(r.Context(), "booking", bookingID, "booking_created", details); err != nil {
		http.Error(w, fmt.Sprintf("booking created but audit log failed: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]any{
		"booking_id": bookingID,
		"status":     status,
		"message":    "Booking confirmed safely with concurrency control",
	})
}

func updateBooking(w http.ResponseWriter, r *http.Request) {
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

	// Simulated booking-updated email.
	time.Sleep(simulatedEmailDelay)
	// Simulated booking-updated SMS.
	time.Sleep(simulatedSMSDelay)

	details := fmt.Sprintf("Booking id=%d updated: room_id=%d, range=%s", updatedID, req.RoomID, dateRange)
	if err := writeAuditLog(r.Context(), "booking", updatedID, "booking_updated", details); err != nil {
		http.Error(w, fmt.Sprintf("booking updated but audit log failed: %v", err), http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(map[string]any{
		"booking_id": updatedID,
		"status":     status,
		"message":    "Booking updated successfully",
	})
}

func cancelBooking(w http.ResponseWriter, r *http.Request) {
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

	// Simulated cancellation email.
	time.Sleep(simulatedEmailDelay)
	// Simulated cancellation SMS.
	time.Sleep(simulatedSMSDelay)

	details := fmt.Sprintf("Booking id=%d cancelled", id)
	if err := writeAuditLog(r.Context(), "booking", id, "booking_cancelled", details); err != nil {
		http.Error(w, fmt.Sprintf("booking cancelled but audit log failed: %v", err), http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(map[string]string{"message": "Booking cancelled successfully"})
}

// --- AVAILABILITY HANDLER ---

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