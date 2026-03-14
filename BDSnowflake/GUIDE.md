## Где я делал лабу

Вывод команды neofetch:

- OS: Kubuntu 22.04.5 LTS x86_64
- Host: NBLK-WAX9X M1010
- Kernel: 6.8.0-101-generic
- Uptime: 1 hour, 57 mins
- Packages: 3404 (dpkg), 126 (brew), 22 (snap)
- Shell: zsh 5.8.1
- Resolution: 1920x1080
- DE: Plasma 5.24.7
- WM: KWin
- WM Theme: Nostrum
- Theme: [Plasma], Breeze [GTK3]
- Icons: Blue-Accent-Icons-for-linux-master [Plasma], Blue-Accent-Icons-for-li
- Terminal: kitty
- CPU: AMD Ryzen 5 3500U with Radeon Vega Mobile Gfx (8) @ 2.100GHz
- GPU: AMD ATI Radeon Vega Series / Radeon Vega Mobile Series
- Memory: 3474MiB / 6865MiB 

Что использовалось в работе:
- редактор: helix
- docker
- база данных: postgres:16
- для работы с бд: pgcli (продвинутый psql; ну и соответственно команды для работы с Postgres внутри оболочки)
- установленные приложения и зависимости (какие будет понятно из "Как и что делать")


## Как и что делать

1. docker-compose.yml
На платформе установлен docker. У меня уже был установлен, гпт говорит, что можно так установить:
 - Обновляем индекс пакетов: sudo apt update
 - Устанавливаем Docker: sudo apt install -y docker.io
 - Устанавливаем плагин Docker Compose (V2): sudo apt install -y docker-compose-plugin
 - (Опционально) Добавляем пользователя в группу docker, чтобы не писать sudo перед каждой командой (потребуется перезагрузка/перезаход): sudo usermod -aG docker $USER

**После установки всего просто делаем docker compose up -d и соответственно все поднимается.**

По идее, на этом этапе будут уже и заполненные данные из файлов mock_data(*).csv, и непосредственно "снежинка".
Как раз файл 01_load.sql  из директории init/ отвечает за Создание основной таблицы и загрузкb 10к строк, а файл 02_snowflake.sql за трансформацию в "снежинку".

Соответственно после docker compose up -d ждем пару секунд, пока Postgres все перенесет (можно отслеживать по: docker logs -f postgres, и там должна быть внизу строчка database system is ready to accept connections). После этого подключаемся к Postgres и там все наши таблички)

**Как подключиться к БД: pgcli postgres://postgres:postgres@localhost:5432/postgres**

### Тут вспомогательная информация, собственно как вообще появился файл 01_load.sql

Как я импортировал данные в бд
Есть 2 рабочих способа:
 1. python3 import_data.py
 должен быть установлен python3, pandas, sqlalchemy, psycopg2-binary. Как устанавливались:  
 - Установка пакетного менеджера (если его нет): sudo apt update && sudo apt install -y python3-pip
 - Установка необходимых библиотек: pip install pandas sqlalchemy psycopg2-binary

Работает, но неидеально определяет типы (например, в столбце с датой тип определил, как TEXT).
По этой причине конкретно я использовал способ №2.

 2. bash ./import_data
 Для работы этого скрипта должен быть установлен csvkit psycopg2-binary. Из csvkit нам конкретно нужен csvsql (позволяет применять SQL-запросы непосредственно к CSV- файлам и переносить их в базы данных (например, SQLite, PostgreSQL)). Собственно, в скрипте import_data.sh он и используется. Как устанавливалось:
 - pip install csvkit psycopg2-binary

P.S. У меня при запуске скрипт кидал предупреждение: RuntimeWarning: Error sniffing CSV dialect: Could not determine delimiter. Оно означает, что он не может «автоматом» учуять разделитель, но ничего страшного в этом нет, все данные копируются корректно.

После этого этапа у нас есть запущенная Postgres в Docker и таблица mock_data на 10000 строк

Ну и теперь у нас есть таблица, в которой у каждого из 50 атрибутов есть тип и мы можем легко написать наш файл 01_load.sql и поместить его в наш docker-compose.yml  


