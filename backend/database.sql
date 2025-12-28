-- =========================
-- ENUM DEFINITIONS (DEMO)
-- =========================

CREATE TYPE event_type_enum AS ENUM (
    'view',
    'click',
    'add_to_cart',
    'purchase',
    'rate'
);

CREATE TYPE cart_status_enum AS ENUM (
    'active',
    'checked_out'
);

CREATE TYPE invoice_status_enum AS ENUM (
    'created',
    'completed',
    'cancelled'
);

-- =========================
-- USERS (AUTH + RS USER)
-- =========================

CREATE TABLE users (
    id              BIGSERIAL PRIMARY KEY,

    username        TEXT UNIQUE,
    password_hash   TEXT,
    phone_number    TEXT,

    amazon_user_id  TEXT UNIQUE,      

    created_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    last_login      TIMESTAMP
);

-- =========================
-- ITEMS (PRODUCT METADATA)
-- =========================

CREATE TABLE products (
    parent_asin     TEXT PRIMARY KEY,

    title           TEXT NOT NULL,
    store           TEXT,
    main_category   TEXT,

    avg_rating      FLOAT,
    rating_number   INT,

    primary_image   TEXT,
    raw_metadata    JSONB,

    created_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE items (
    asin            TEXT PRIMARY KEY,
    parent_asin     TEXT NOT NULL REFERENCES products(parent_asin),

    variant         TEXT,           
    primary_image   TEXT,
    category        TEXT,            -- Category từ semantic attributes

    created_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

-- =========================
-- ITEM IMAGES (1-N)
-- =========================

CREATE TABLE item_images (
    id          BIGSERIAL PRIMARY KEY,
    asin        TEXT NOT NULL REFERENCES items(asin),

    image_url   TEXT NOT NULL,
    variant     TEXT,                  
    is_primary  BOOLEAN NOT NULL DEFAULT FALSE
);

-- =========================
-- REVIEWS (AMAZON DATASET)
-- =========================

CREATE TABLE reviews (
    id              BIGSERIAL PRIMARY KEY,

    user_id         BIGINT REFERENCES users(id),
    asin            TEXT REFERENCES items(asin),

    rating          FLOAT NOT NULL,
    review_title    TEXT,
    review_text     TEXT,

    helpful_vote    INT NOT NULL DEFAULT 0,
    verified        BOOLEAN NOT NULL DEFAULT FALSE,

    review_ts       TIMESTAMP NOT NULL
);

-- =========================
-- INTERACTION LOGS (REALTIME EVENTS)
-- =========================

CREATE TABLE interaction_logs (
    id          BIGSERIAL PRIMARY KEY,

    user_id     BIGINT NOT NULL REFERENCES users(id),
    asin        TEXT NOT NULL REFERENCES items(asin),

    event_type  event_type_enum NOT NULL,
    ts          TIMESTAMP NOT NULL DEFAULT NOW(),

    metadata    JSONB
);

-- =========================
-- SHOPPING CART (DEMO FLOW)
-- =========================

CREATE TABLE shopping_carts (
    id          BIGSERIAL PRIMARY KEY,
    user_id     BIGINT NOT NULL REFERENCES users(id),

    status      cart_status_enum NOT NULL DEFAULT 'active',
    created_at  TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE cart_items (
    cart_id     BIGINT NOT NULL REFERENCES shopping_carts(id),
    asin        TEXT NOT NULL REFERENCES items(asin),
    quantity    INT NOT NULL DEFAULT 1,

    PRIMARY KEY (cart_id, asin)
);

-- =========================
-- INVOICES (CHECKOUT DEMO)
-- =========================

CREATE TABLE invoices (
    id              BIGSERIAL PRIMARY KEY,
    user_id         BIGINT NOT NULL REFERENCES users(id),

    total_items     INT NOT NULL,
    status          invoice_status_enum NOT NULL DEFAULT 'created',

    created_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE invoice_items (
    invoice_id      BIGINT NOT NULL REFERENCES invoices(id),
    asin            TEXT NOT NULL REFERENCES items(asin),
    quantity        INT NOT NULL DEFAULT 1,

    PRIMARY KEY (invoice_id, asin)
);

-- =========================
-- MODEL REGISTRY (ARTIFACT MANAGEMENT)
-- =========================

CREATE TABLE model_registry (
    id              BIGSERIAL PRIMARY KEY,

    model_name      TEXT NOT NULL,
    version         TEXT NOT NULL,
    artifact_path   TEXT NOT NULL,

    trained_at      TIMESTAMP NOT NULL,
    notes           TEXT
);

CREATE INDEX idx_reviews_user ON reviews(user_id);
CREATE INDEX idx_reviews_asin ON reviews(asin);

CREATE INDEX idx_logs_user_ts ON interaction_logs(user_id, ts);
CREATE INDEX idx_logs_asin ON interaction_logs(asin);