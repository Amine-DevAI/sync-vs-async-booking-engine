-- Enable the BTree-GiST extension for range exclusion constraints
CREATE EXTENSION IF NOT EXISTS btree_gist;

-- 1. Users Table
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    email TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 2. Rooms Table
CREATE TABLE rooms (
    id SERIAL PRIMARY KEY,
    room_number TEXT UNIQUE NOT NULL,
    room_type TEXT NOT NULL, -- e.g., 'suite', 'standard'
    price_per_night NUMERIC(10, 2) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 3. Bookings Table (The Core Engine with Concurrency Control)
CREATE TABLE bookings (
    id SERIAL PRIMARY KEY,
    user_id INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    room_id INT NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
    status TEXT NOT NULL DEFAULT 'confirmed', -- 'confirmed', 'cancelled'
    
    -- Using PostgreSQL TSRANGE to represent the check-in to check-out window
    during TSRANGE NOT NULL,
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,

    -- MATHEMATICAL GUARANTEE: Prevent overlapping date ranges for the same room
    EXCLUDE USING GIST (
        room_id WITH =,
        during WITH &&
    ) WHERE (status != 'cancelled')
);