USE kafkapipeline;
CREATE TABLE IF NOT EXISTS kafkapipeline.fakerdata (
  user_id UUID,
  username TEXT,
  name TEXT,
  email TEXT,
  phone TEXT,
  birthdate DATE,
  job TEXT,
  company TEXT,
  address TEXT,
  city TEXT,
  postcode TEXT,
  country TEXT,
  passport_number TEXT,
  credit_card_provider TEXT,
  credit_card_number TEXT,
  credit_card_expire TEXT,
  ip_address TEXT,
  user_agent TEXT,
  registered_at TIMESTAMP,
  PRIMARY KEY (user_id)
);
