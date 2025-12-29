"""
Script để fix database về local và kiểm tra PostgreSQL
"""
import os
import sys
from pathlib import Path

# Fix encoding cho Windows
if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

BASE_DIR = Path(__file__).resolve().parent.parent
ENV_FILE = BASE_DIR / ".env"

def check_and_fix_env():
    """Kiểm tra và fix file .env"""
    print("=" * 60)
    print("KIEM TRA VA FIX DATABASE CONFIG")
    print("=" * 60)
    
    if not ENV_FILE.exists():
        print(f"\n[OK] Khong co file .env")
        return
    
    print(f"\n[INFO] Tim thay file .env: {ENV_FILE}")
    
    # Đọc file .env
    lines = []
    has_database_url = False
    
    with open(ENV_FILE, 'r', encoding='utf-8') as f:
        for line in f:
            if line.strip().startswith('DATABASE_URL'):
                has_database_url = True
                print(f"[WARNING] Tim thay DATABASE_URL trong .env:")
                print(f"   {line.strip()}")
                print(f"[INFO] Comment out dong nay de dung local database")
                # Comment out dòng này
                lines.append(f"# {line.strip()}  # Commented out - using local database from config.py\n")
            else:
                lines.append(line)
    
    if has_database_url:
        # Backup file cũ
        backup_file = ENV_FILE.with_suffix('.env.backup')
        import shutil
        shutil.copy2(ENV_FILE, backup_file)
        print(f"[INFO] Da backup file .env thanh: {backup_file}")
        
        # Ghi lại file
        with open(ENV_FILE, 'w', encoding='utf-8') as f:
            f.writelines(lines)
        print(f"[OK] Da comment out DATABASE_URL trong .env")
        print(f"[INFO] Bay gio se dung local database tu config.py")
    else:
        print(f"[OK] Khong co DATABASE_URL trong .env")

def check_postgresql():
    """Kiểm tra PostgreSQL có chạy không"""
    print("\n" + "=" * 60)
    print("KIEM TRA POSTGRESQL")
    print("=" * 60)
    
    # Kiểm tra port 5432
    import socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(1)
    result = sock.connect_ex(('localhost', 5432))
    sock.close()
    
    if result == 0:
        print("\n[OK] PostgreSQL dang chay tren port 5432")
        return True
    else:
        print("\n[ERROR] PostgreSQL KHONG chay tren port 5432")
        print("\n[HUONG DAN] Khoi dong PostgreSQL:")
        print("1. Mo Services (services.msc)")
        print("2. Tim service 'postgresql-x64-XX' hoac 'PostgreSQL'")
        print("3. Click phai > Start")
        print("\nHoac chay lenh PowerShell:")
        print("   Get-Service -Name '*postgres*' | Start-Service")
        print("\nHoac neu chua cai dat PostgreSQL:")
        print("   - Download tu: https://www.postgresql.org/download/windows/")
        print("   - Hoac dung Docker: docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=VanLinh04 postgres")
        return False

def main():
    """Hàm chính"""
    check_and_fix_env()
    pg_running = check_postgresql()
    
    print("\n" + "=" * 60)
    if pg_running:
        print("[OK] Co the ket noi den PostgreSQL")
        print("\nChay lenh sau de test ket noi:")
        print("   python backend/scripts/check_db_connection.py")
    else:
        print("[ERROR] Can khoi dong PostgreSQL truoc")
    print("=" * 60)

if __name__ == "__main__":
    main()

