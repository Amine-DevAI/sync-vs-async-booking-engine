package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"fmt"
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