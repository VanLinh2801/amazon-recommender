-- Migration: Add category column to items table
-- Date: 2024
-- Description: Thêm cột category vào bảng items để lưu category từ semantic attributes

-- Thêm cột category nếu chưa tồn tại
DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 
        FROM information_schema.columns 
        WHERE table_name = 'items' 
        AND column_name = 'category'
    ) THEN
        ALTER TABLE items ADD COLUMN category TEXT;
        CREATE INDEX idx_items_category ON items(category);
        RAISE NOTICE 'Column category đã được thêm vào bảng items';
    ELSE
        RAISE NOTICE 'Column category đã tồn tại trong bảng items';
    END IF;
END $$;

