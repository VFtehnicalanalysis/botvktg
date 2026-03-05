# botvktg systemd

Подготовленные юниты:
- `botvktg.service` — основной сервис бота (автоперезапуск + env-параметры).
- `botvktg-daily-restart.service` — помечает `planned_daily` и перезапускает основной сервис.
- `botvktg-daily-restart.timer` — запускает плановый рестарт каждый день в `09:30` по `Europe/Moscow`.

Дополнительно в коде:
- при аварийном падении бот отправляет OWNER `.txt` с логами за последние 3 минуты до падения;
- при рестарте по неактивности мониторинга (`>5 минут`) или по таймеру приходит отдельное уведомление OWNER.

## Установка

```bash
sudo cp /Users/vf/Documents/Projects/botvktg/deploy/systemd/botvktg.service /etc/systemd/system/
sudo cp /Users/vf/Documents/Projects/botvktg/deploy/systemd/botvktg-daily-restart.service /etc/systemd/system/
sudo cp /Users/vf/Documents/Projects/botvktg/deploy/systemd/botvktg-daily-restart.timer /etc/systemd/system/

sudo systemctl daemon-reload
sudo systemctl enable --now botvktg.service
sudo systemctl enable --now botvktg-daily-restart.timer
```

## Проверка

```bash
sudo systemctl status botvktg.service
sudo systemctl status botvktg-daily-restart.timer
sudo journalctl -u botvktg.service -f
systemctl list-timers --all | grep botvktg-daily-restart
```

## Ручной плановый перезапуск (с уведомлением OWNER)

```bash
sudo systemctl start botvktg-daily-restart.service
```

## Обновление кода и перезапуск

```bash
cd /Users/vf/Documents/Projects/botvktg
# обновите код/ветку
sudo systemctl restart botvktg.service
```

## Важно

Перед установкой проверьте и при необходимости измените в `botvktg.service`:
- `User=` / `Group=`
- `WorkingDirectory=`
- `ExecStart=`
- `RESTART_REASON_PATH=`
