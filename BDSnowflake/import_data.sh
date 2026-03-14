#!/bin/bash

echo "Создаем таблицу и загружаем первый файл..."
csvsql --db postgresql://postgres:postgres@localhost:5432/postgres \
  --insert --overwrite --tables mock_data -d "," "./data/MOCK_DATA.csv"

for i in {1..9}; do
    echo "Загрузка файла ($i)..."
    csvsql --db postgresql://postgres:postgres@localhost:5432/postgres \
      --insert --no-create --tables mock_data -d "," "./data/MOCK_DATA ($i).csv"
done

