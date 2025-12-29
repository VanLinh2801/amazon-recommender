# Script để khởi động PostgreSQL service
# Chạy với quyền Administrator: Right-click > Run as Administrator

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "KHOI DONG POSTGRESQL SERVICE" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

# Kiểm tra service
$service = Get-Service -Name "postgresql-x64-17" -ErrorAction SilentlyContinue

if (-not $service) {
    Write-Host "[ERROR] Khong tim thay service postgresql-x64-17" -ForegroundColor Red
    Write-Host "Kiem tra ten service:" -ForegroundColor Yellow
    Get-Service -Name "*postgres*" | Format-Table Name, Status, DisplayName
    exit 1
}

Write-Host "`n[INFO] Tim thay service: $($service.Name)" -ForegroundColor Green
Write-Host "       Status hien tai: $($service.Status)" -ForegroundColor Yellow

if ($service.Status -eq 'Running') {
    Write-Host "`n[OK] PostgreSQL da dang chay!" -ForegroundColor Green
    exit 0
}

# Thử khởi động service
Write-Host "`n[INFO] Dang khoi dong service..." -ForegroundColor Yellow

try {
    Start-Service -Name "postgresql-x64-17" -ErrorAction Stop
    Start-Sleep -Seconds 3
    
    $service = Get-Service -Name "postgresql-x64-17"
    if ($service.Status -eq 'Running') {
        Write-Host "[OK] PostgreSQL da khoi dong thanh cong!" -ForegroundColor Green
        
        # Kiểm tra port
        Write-Host "`n[INFO] Dang kiem tra port 5432..." -ForegroundColor Yellow
        $portTest = Test-NetConnection -ComputerName localhost -Port 5432 -InformationLevel Quiet -WarningAction SilentlyContinue
        
        if ($portTest) {
            Write-Host "[OK] Port 5432 dang mo!" -ForegroundColor Green
        } else {
            Write-Host "[WARNING] Port 5432 chua mo, co the can doi them..." -ForegroundColor Yellow
        }
    } else {
        Write-Host "[ERROR] Service khong khoi dong duoc. Status: $($service.Status)" -ForegroundColor Red
        Write-Host "`n[HUONG DAN]:" -ForegroundColor Yellow
        Write-Host "1. Mo Services (Win+R > services.msc)" -ForegroundColor White
        Write-Host "2. Tim 'postgresql-x64-17'" -ForegroundColor White
        Write-Host "3. Click phai > Start" -ForegroundColor White
        exit 1
    }
} catch {
    Write-Host "[ERROR] Loi khi khoi dong service: $_" -ForegroundColor Red
    Write-Host "`n[GIAI PHAP]:" -ForegroundColor Yellow
    Write-Host "1. Chay PowerShell voi quyền Administrator" -ForegroundColor White
    Write-Host "2. Hoac mo Services (Win+R > services.msc) va start thu cong" -ForegroundColor White
    exit 1
}

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "HOAN TAT!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Cyan



