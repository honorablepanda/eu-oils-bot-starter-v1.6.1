PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS websites (
  website_id     TEXT PRIMARY KEY,  -- e.g., 'be:carrefour.be'
  country        TEXT NOT NULL,     -- ISO2
  chain          TEXT NOT NULL,     -- Human-readable name
  site_domain    TEXT NOT NULL,     -- registrable domain, e.g. 'carrefour.be'
  retailer_class TEXT NOT NULL CHECK (retailer_class IN ('grocery','beauty','pharmacy','marketplace')),
  robots_status  TEXT NOT NULL CHECK (robots_status IN ('allowed','blocked','review')),
  priority       TEXT NOT NULL CHECK (priority IN ('must_cover','long_tail')),
  notes          TEXT DEFAULT ''
);

-- Uniqueness within a country
CREATE UNIQUE INDEX IF NOT EXISTS uq_websites_country_domain
  ON websites(country, site_domain);
