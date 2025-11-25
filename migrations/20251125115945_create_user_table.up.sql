CREATE TABLE IF NOT EXISTS users (
  user_id INT PRIMARY KEY,
  signup_date DATE NOT NULL,
  city VARCHAR(100) NOT NULL,
  device_type VARCHAR(50) NOT NULL
);
