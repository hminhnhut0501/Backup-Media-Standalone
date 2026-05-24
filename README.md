# Backup Media Standalone

Project này tách riêng module `backup media` từ `tele_v4` để chạy độc lập.

## Chạy nhanh

```bash
cd /Users/hminhnhut/backup_media_standalone
./run.sh
```

Mặc định chạy tại `http://localhost:8010`.

## Cấu hình Telegram

Ưu tiên dùng biến môi trường:

- `TG_API_ID`
- `TG_API_HASH`
- `TG_STRING_SESSION` (khuyến nghị)

Hoặc dùng `config_filter.ini` + file session path như trong cấu hình.

## Phụ thuộc hệ thống

- Cần có `ffmpeg` và `ffprobe` trong PATH để xử lý video.
