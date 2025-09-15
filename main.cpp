#include <iostream>
#include <fstream>
#include <sstream>
#include <cstring>
#include <mutex>
#include <string>
#include <regex>
#include "HashTable.h"  
#include "nlohmann/json.hpp"  
#include <algorithm>
#include <thread>
#include <chrono>

using namespace std;
using json = nlohmann::json;
namespace fs = std::filesystem;

// Самописная структура для хранения вектора
template<typename T>
struct CustVector {
    T* data;  // Указатель на данные
    size_t size;  // Текущий размер вектора
    size_t capacity;  // Вместимость вектора

    CustVector() : data(nullptr), size(0), capacity(0) {}  // Конструктор по умолчанию

    CustVector(const CustVector& other) {  // Конструктор копирования
        size = other.size;
        capacity = other.capacity;
        data = new T[capacity];
        for (size_t i = 0; i < size; ++i) {
            data[i] = other.data[i];
        }
    }

    CustVector& operator=(const CustVector& other) {  // Оператор присваивания
        if (this != &other) {
            delete[] data;
            size = other.size;
            capacity = other.capacity;
            data = new T[capacity];
            for (size_t i = 0; i < size; ++i) {
                data[i] = other.data[i];
            }
        }
        return *this;
    }

    ~CustVector() {  // Деструктор
        delete[] data;
    }

    void push_back(const T& value) {  // Добавление элемента в конец вектора
        if (size == capacity) {
            capacity = capacity == 0 ? 1 : capacity * 2;
            T* new_data = new T[capacity];
            for (size_t i = 0; i < size; ++i) {
                new_data[i] = data[i];
            }
            delete[] data;
            data = new_data;
        }
        data[size++] = value;
    }

    T& operator[](size_t index) {  // Оператор доступа по индексу
        return data[index];
    }

    const T& operator[](size_t index) const {  // Константный оператор доступа по индексу
        return data[index];
    }
};

// Структуры для хранения таблицы
struct Table {
    string name;  // Имя таблицы
    CustVector<string> columns;  // Столбцы таблицы
    CustVector<CustVector<string>> rows;  // Строки таблицы
    string primary_key;  // Первичный ключ
    string file_format; // Формат файла в котором хранится таблица
    size_t pk_sequence;  // Последовательность для первичного ключа
    mutex lock;  // Мьютекс для обеспечения потокобезопасности

    Table(const string& n) : name(n), pk_sequence(0) {}  // Конструктор с именем таблицы

    Table(const Table& other)  // Конструктор копирования
        : name(other.name), columns(other.columns), rows(other.rows), primary_key(other.primary_key), pk_sequence(other.pk_sequence) {
    }

    Table& operator=(const Table& other) {  // Оператор присваивания
        if (this != &other) {
            name = other.name;
            columns = other.columns;
            rows = other.rows;
            primary_key = other.primary_key;
            pk_sequence = other.pk_sequence;
        }
        return *this;
    }
};

// Карта для хранения таблиц
HashTable tables(10);  // Хеш-таблица для хранения таблиц

string trim(const string& str) {
    size_t first = str.find_first_not_of(" \t\n\r\f\v");
    if (string::npos == first) {
        return ""; // Возвращаем пустую строку если только пробельные символы
    }
    size_t last = str.find_last_not_of(" \t\n\r\f\v");
    return str.substr(first, last - first + 1);
}

void lock_table(const string& data_dir, const string& table_name) {
    fs::path lock_file_path = fs::path(data_dir) / (table_name + "_lock.txt");
    ofstream lock_file(lock_file_path);
    if (!lock_file.is_open()) {
        cerr << "Failed to open lock file for writing: " << lock_file_path << endl;
        return;
    }
    lock_file << "lock";
    lock_file.close();
}

void unlock_table(const string& data_dir, const string& table_name) {
    fs::path lock_file_path = fs::path(data_dir) / (table_name + "_lock.txt");
    ofstream lock_file(lock_file_path);
    if (!lock_file.is_open()) {
        cerr << "Failed to open lock file for writing: " << lock_file_path << endl;
        return;
    }
    lock_file << "unlock";
    lock_file.close();
}

void wait_for_unlock(const string& data_dir, const string& table_name) {
    while (true) {
        fs::path lock_file_path = fs::path(data_dir) / (table_name + "_lock.txt");
        ifstream lock_file(lock_file_path);
        if (!lock_file.is_open()) {
            cerr << "Failed to open lock file for reading: " << lock_file_path << endl;
            return;
        }
        string state;
        getline(lock_file, state);
        lock_file.close();

        if (state == "unlock") {
            lock_table(data_dir, table_name);
            break;
        }

        // Ждем немного перед следующей проверкой
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
}

size_t read_pk_sequence(const string& data_dir, const string& table_name) {
    fs::path pk_file_path = fs::path(data_dir) / (table_name + "_pk_sequence.txt");
    ifstream pk_file(pk_file_path);
    if (!pk_file.is_open()) {
        cerr << "Failed to open pk_sequence file for reading: " << pk_file_path << endl;
        return 0;
    }
    size_t pk_sequence;
    pk_file >> pk_sequence;
    pk_file.close();
    return pk_sequence;
}

void write_pk_sequence(const string& data_dir, const string& table_name, size_t pk_sequence) {
    fs::path pk_file_path = fs::path(data_dir) / (table_name + "_pk_sequence.txt");
    ofstream pk_file(pk_file_path);
    if (!pk_file.is_open()) {
        cerr << "Failed to open pk_sequence file for writing: " << pk_file_path << endl;
        return;
    }
    pk_file << pk_sequence;
    pk_file.close();
}

void save_table_json(const string& data_dir, const Table& table) {
    fs::path file_path = fs::path(data_dir) / (table.name + ".json");
    ofstream file(file_path);
    if (!file.is_open()) {
        cout << "Failed to open file for writing: " << file_path << endl;
        return;
    }

    json j;
    j["name"] = table.name;
    j["columns"] = json::array();
    for (size_t i = 0; i < table.columns.size; ++i) {
        j["columns"].push_back(table.columns[i]);
    }
    j["rows"] = json::array();
    for (size_t i = 0; i < table.rows.size; ++i) {
        json row = json::array();
        for (size_t j = 0; j < table.rows[i].size; ++j) {
            row.push_back(table.rows[i][j]);
        }
        j["rows"].push_back(row);
    }
    j["primary_key"] = table.primary_key;
    file << j.dump(4);
}

void load_table_json(const string& data_dir, const string& table_name) {
    fs::path file_path = fs::path(data_dir) / (table_name + ".json");
    ifstream file(file_path);
    if (!file.is_open()) {
        cout << "File not found: " << file_path << endl;
        return;
    }

    json j;
    file >> j;

    Table* table_ptr = new Table(table_name);
    for (const auto& col : j["columns"]) {
        table_ptr->columns.push_back(col);
    }
    for (const auto& row : j["rows"]) {
        CustVector<string> row_data;
        for (const auto& val : row) {
            row_data.push_back(val);
        }
        table_ptr->rows.push_back(row_data);
    }
    table_ptr->primary_key = j["primary_key"];
    table_ptr->file_format = "json"; // Устанавливаем формат файла

    tables.put(table_name, reinterpret_cast<void*>(table_ptr));
}

// Загрузка таблицы из CSV с указанием директории
void load_table_csv(const string& data_dir, const string& table_name) {
    fs::path file_path = fs::path(data_dir) / (table_name + ".csv");
    ifstream file(file_path);
    if (!file.is_open()) {
        cout << "File not found: " << file_path << endl;
        return;
    }

    Table* table_ptr = new Table(table_name);
    string line;
    bool first_line = true;
    bool has_primary_key_info = false;
    string primary_key_from_csv;

    while (getline(file, line)) {
        istringstream iss(line);
        string value;
        CustVector<string> row;

        while (getline(iss, value, ',')) {
            if (value.front() == '"' && value.back() == '"') {
                value = value.substr(1, value.size() - 2);
            }
            row.push_back(value);
        }

        if (first_line) {
            // Проверяем, есть ли информация о первичном ключе в последнем элементе
            if (row.size > 0) {
                string last_column = row[row.size - 1];
                if (last_column.find("PRIMARY_KEY:") == 0) {
                    // Извлекаем первичный ключ
                    primary_key_from_csv = last_column.substr(12);
                    has_primary_key_info = true;
                    // Удаляем информацию о PK из столбцов
                    CustVector<string> cleaned_columns;
                    for (size_t i = 0; i < row.size - 1; ++i) {
                        cleaned_columns.push_back(row[i]);
                    }
                    table_ptr->columns = cleaned_columns;
                } else {
                    table_ptr->columns = row;
                }
            }
            first_line = false;
        }
        else {
            table_ptr->rows.push_back(row);
        }
    }

    // Устанавливаем первичный ключ
    if (has_primary_key_info) {
        table_ptr->primary_key = primary_key_from_csv;
    } else {
        // По умолчанию первым столбцом является ID
        table_ptr->primary_key = "ID";
    }

    // Проверяем и корректируем индексы (должны начинаться с 1)
    for (size_t i = 0; i < table_ptr->rows.size; ++i) {
        if (table_ptr->rows[i].size > 0) {
            try {
                int current_id = stoi(table_ptr->rows[i][0]);
                if (current_id == 0) {
                    // Если ID = 0, меняем на i+1
                    table_ptr->rows[i][0] = to_string(i + 1);
                }
            } catch (...) {
                // Если не число, устанавливаем правильный ID
                table_ptr->rows[i][0] = to_string(i + 1);
            }
        }
    }

    table_ptr->file_format = "csv"; // Устанавливаем формат файла
    tables.put(table_name, reinterpret_cast<void*>(table_ptr));
}

void load_table(const string& data_dir, const string& table_name) {
    fs::path json_path = fs::path(data_dir) / (table_name + ".json");
    fs::path csv_path = fs::path(data_dir) / (table_name + ".csv");

    bool json_exists = fs::exists(json_path);
    bool csv_exists = fs::exists(csv_path);

    if (json_exists && csv_exists) {
        cout << "Error: Both JSON and CSV files exist for table '" << table_name << "'" << endl;
        return;
    }

    if (json_exists) {
        load_table_json(data_dir, table_name);
    }
    else if (csv_exists) {
        load_table_csv(data_dir, table_name);
    }
    else {
        cerr << "Error: No table file found for '" << table_name << "' in directory '" << data_dir << "'" << endl;
        return;
    }
}


void save_table_csv(const string& data_dir, const Table& table) {
    fs::path file_path = fs::path(data_dir) / (table.name + ".csv");
    ofstream file(file_path);
    if (!file.is_open()) {
        cout << "Failed to open file for writing: " << file_path << endl;
        return;
    }

    // Запись заголовков с информацией о первичном ключе в конце
    for (size_t i = 0; i < table.columns.size; ++i) {
        file << "\"" << table.columns[i] << "\"";
        if (i < table.columns.size - 1) file << ",";
    }
    // Добавляем информацию о первичном ключе в конец первой строки
    file << ",\"PRIMARY_KEY:" << table.primary_key << "\"";
    file << endl;

    // Запись данных
    for (size_t i = 0; i < table.rows.size; ++i) {
        for (size_t j = 0; j < table.rows[i].size; ++j) {
            file << "\"" << table.rows[i][j] << "\"";
            if (j < table.rows[i].size - 1) {
                file << ",";
            }
        }
        file << endl;
    }
    cout << "Table saved to " << file_path << endl;
}

// Сохранить таблицу в CSV формате (удаляя JSON если был)
void save_as_csv(const string& data_dir, const string& table_name, const Table& table) {
    fs::path json_path = fs::path(data_dir) / (table_name + ".json");
    fs::path csv_path = fs::path(data_dir) / (table_name + ".csv");
    
    // Если уже в CSV - ничего не делаем
    if (fs::exists(csv_path) && !fs::exists(json_path)) {
        cout << "Table is already in CSV format: " << csv_path << endl;
        return;
    }
    wait_for_unlock(data_dir, table_name);
    
    // Удаляем старый JSON файл если существует
    if (fs::exists(json_path)) {
        fs::remove(json_path);
        cout << "Removed JSON file: " << json_path << endl;
    }
    
    // Сохраняем в CSV
    save_table_csv(data_dir, table);
    unlock_table(data_dir, table_name);
    cout << "Table saved as CSV: " << csv_path << endl;
}

// Сохранить таблицу в JSON формате (удаляя CSV если был)
void save_as_json(const string& data_dir, const string& table_name, const Table& table) {
    fs::path json_path = fs::path(data_dir) / (table_name + ".json");
    fs::path csv_path = fs::path(data_dir) / (table_name + ".csv");
    
    // Если уже в JSON - ничего не делаем
    if (fs::exists(json_path) && !fs::exists(csv_path)) {
        cout << "Table is already in JSON format: " << json_path << endl;
        return;
    }
    wait_for_unlock(data_dir, table_name);
    
    // Удаляем старый CSV файл если существует
    if (fs::exists(csv_path)) {
        fs::remove(csv_path);
        cout << "Removed CSV file: " << csv_path << endl;
    }
    
    // Сохраняем в JSON
    save_table_json(data_dir, table);
    unlock_table(data_dir, table_name);
    cout << "Table saved as JSON: " << json_path << endl;
}

// Функция для сохранения последовательности первичных ключей
void save_pk_sequence(const Table& table) {
    ofstream file(table.name + "_pk_sequence.txt");
    if (!file.is_open()) {
        cout << "Failed to open file for writing pk_sequence." << endl;
        return;
    }
    file << table.pk_sequence;
    cout << "Primary key sequence saved to " << table.name << "_pk_sequence.txt" << endl;
}

// Функция для загрузки последовательности первичных ключей
void load_pk_sequence(Table& table) {
    ifstream file(table.name + "_pk_sequence.txt");
    if (!file.is_open()) {
        cout << "File not found for pk_sequence." << endl;
        return;
    }
    file >> table.pk_sequence;
    cout << "Primary key sequence loaded from " << table.name << "_pk_sequence.txt" << endl;
}

// Вспомогательная функция для добавления уникальной колонки
void add_column_if_unique(const string& column, CustVector<string>& columns) {
    if (column.empty() || column == "ID") {
        return;
    }
    
    // Проверяем уникальность (без unordered_set)
    for (size_t i = 0; i < columns.size; ++i) {
        if (columns[i] == column) {
            return; // Колонка уже существует
        }
    }
    
    // Добавляем если уникальна
    columns.push_back(column);
}

void create_table(const string& data_dir, const string& table_name, const CustVector<string>& columns, const string& primary_key) {
    // Быстрая проверка файлов
    const string base_path = data_dir + "/" + table_name;
    if (fs::exists(base_path + ".json") ||
        fs::exists(base_path + ".csv") ||
        fs::exists(base_path + "_lock.txt") ||
        fs::exists(base_path + "_pk_sequence.txt")) {
        cout << "Error: Table files already exist for: " << table_name << endl;
        return;
    }

    // Создание таблицы
    Table new_table(table_name);

    // Добавляем обязательную колонку с именем из primary_key
    new_table.columns.push_back(primary_key);

    // Обрабатываем колонки
    for (size_t i = 0; i < columns.size; ++i) {
        string column_str = trim(columns[i]);

        // Пропускаем пустые строки
        if (column_str.empty()) {
            continue;
        }

        // Если колонка содержит запятые, разбиваем ее
        if (column_str.find(',') != string::npos) {
            size_t start = 0;
            size_t end = column_str.find(',');

            while (end != string::npos) {
                string single_column = trim(column_str.substr(start, end - start));
                add_column_if_unique(single_column, new_table.columns);
                start = end + 1;
                end = column_str.find(',', start);
            }

            // Последняя колонка после последней запятой
            string last_column = trim(column_str.substr(start));
            add_column_if_unique(last_column, new_table.columns);
        } else {
            // Одиночная колонка
            add_column_if_unique(column_str, new_table.columns);
        }
    }

    // Проверяем, что есть хотя бы одна колонка кроме первичного ключа
    if (new_table.columns.size <= 1) {
        cout << "Error: Table must have at least one column besides primary key" << endl;
        return;
    }

    const string trimmed_primary_key = trim(primary_key);
    // Устанавливаем первичный ключ
    new_table.primary_key = trimmed_primary_key;
    // Устанавливаем формат по умолчанию (например, JSON)
    new_table.file_format = "json";

    // Сохраняем в JSON файл
    save_table_json(data_dir, new_table);

    // Создаем файл блокировки
    ofstream lock_file(base_path + "_lock.txt");
    if (lock_file.is_open()) {
        lock_file << "unlock";
        lock_file.close();
    }

    // Создаем файл последовательности первичных ключей
    ofstream pk_seq_file(base_path + "_pk_sequence.txt");
    if (pk_seq_file.is_open()) {
        pk_seq_file << "0";
        pk_seq_file.close();
    }

    // Сохраняем в хеш-таблицу
    Table* saved_table = new Table(new_table);
    saved_table->file_format = "json"; // Устанавливаем формат
    tables.put(table_name, reinterpret_cast<void*>(saved_table));

    // Выводим информацию о созданной таблице
    cout << "Table '" << table_name << "' created successfully!" << endl;
}

void insert_data(const string& data_dir, const string& table_name, const CustVector<string>& values) {
    wait_for_unlock(data_dir, table_name);

    Table* table = reinterpret_cast<Table*>(tables.get(table_name));
    if (!table) {
        unlock_table(data_dir, table_name);
        cout << "Table not found." << endl;
        return;
    }

    // Чтение текущей последовательности первичных ключей
    size_t pk_sequence = read_pk_sequence(data_dir, table_name);
    string pk_value = to_string(pk_sequence + 1);

    CustVector<string> new_row;

    // Находим индекс первичного ключа
    size_t pk_index = 0;
    for (size_t i = 0; i < table->columns.size; ++i) {
        if (table->columns[i] == table->primary_key) {
            pk_index = i;
            break;
        }
    }

    // Создаем новую строку
    for (size_t i = 0; i < table->columns.size; ++i) {
        if (i == pk_index) {
            new_row.push_back(pk_value);
        } else {
            if (i - (i > pk_index ? 1 : 0) < values.size) {
                string value = values[i - (i > pk_index ? 1 : 0)];
                if (value.front() == '(') value = value.substr(1);
                if (value.back() == ')') value = value.substr(0, value.size() - 1);
                new_row.push_back(value);
            } else {
                new_row.push_back("");
            }
        }
    }

    table->rows.push_back(new_row);

    // Обновление последовательности первичных ключей
    write_pk_sequence(data_dir, table_name, pk_sequence + 1);

    // Сохраняем в том же формате, в котором была загружена таблица
    if (table->file_format == "json") {
        save_table_json(data_dir, *table);
    } else if (table->file_format == "csv") {
        save_table_csv(data_dir, *table);
    } else {
        // По умолчанию сохраняем в JSON
        save_table_json(data_dir, *table);
    }

    unlock_table(data_dir, table_name);
    cout << "Data inserted successfully." << endl;
}


// Функция для проверки простого условия
bool check_simple_condition(const Table* table, size_t row_index, const string& condition) {
    regex condition_regex(R"((\w+)\s*(=|<|>|<=|>=|!=)\s*('[^']*'|"[^"]*"|\d+))");
    smatch match;
    if (!regex_search(condition, match, condition_regex)) {
        return false;
    }

    string col = match[1];
    string op = match[2];
    string val = match[3];

    // Удаление лишних кавычек из значения
    if (val.front() == '\'' || val.front() == '"') {
        val = val.substr(1, val.size() - 2);
    }

    // Ищем столбец и проверяем условие
    for (size_t j = 0; j < table->columns.size; ++j) {
        if (table->columns[j] == col) {
            string cell_value = table->rows[row_index][j];
            
            if (op == "=" && cell_value == val) return true;
            else if (op == "!=" && cell_value != val) return true;
            else if (op == "<") {
                try {
                    double cell_num = stod(cell_value);
                    double val_num = stod(val);
                    if (cell_num < val_num) return true;
                } catch (...) {
                    if (cell_value < val) return true;
                }
            }
            else if (op == ">") {
                try {
                    double cell_num = stod(cell_value);
                    double val_num = stod(val);
                    if (cell_num > val_num) return true;
                } catch (...) {
                    if (cell_value > val) return true;
                }
            }
            else if (op == "<=") {
                try {
                    double cell_num = stod(cell_value);
                    double val_num = stod(val);
                    if (cell_num <= val_num) return true;
                } catch (...) {
                    if (cell_value <= val) return true;
                }
            }
            else if (op == ">=") {
                try {
                    double cell_num = stod(cell_value);
                    double val_num = stod(val);
                    if (cell_num >= val_num) return true;
                } catch (...) {
                    if (cell_value >= val) return true;
                }
            }
            break;
        }
    }
    return false;
}

// Функция для поиска внешних операторов (игнорируя те, что в скобках)
size_t find_outer_operator(const string& condition, const string& op) {
    int bracket_level = 0;
    size_t pos = 0;
    
    while ((pos = condition.find(op, pos)) != string::npos) {
        // Проверяем что не внутри скобок
        string before = condition.substr(0, pos);
        int open_brackets = std::count(before.begin(), before.end(), '(');
        int close_brackets = std::count(before.begin(), before.end(), ')');
        
        if (open_brackets == close_brackets) {
            return pos; // Нашли внешний оператор
        }
        pos += op.length();
    }
    return string::npos;
}

// Рекурсивная функция для проверки сложных условий
bool check_complex_condition(const Table* table, size_t row_index, const string& condition) {
    string cond = trim(condition);
    if (cond.empty()) return true;

    // Ищем самый внешний OR (имеет lowest priority)
    size_t or_pos = find_outer_operator(cond, " OR ");
    if (or_pos != string::npos) {
        string left = trim(cond.substr(0, or_pos));
        string right = trim(cond.substr(or_pos + 4));
        return check_complex_condition(table, row_index, left) || 
               check_complex_condition(table, row_index, right);
    }

    // Ищем самый внешний AND (приоритет выше чем OR)
    size_t and_pos = find_outer_operator(cond, " AND ");
    if (and_pos != string::npos) {
        string left = trim(cond.substr(0, and_pos));
        string right = trim(cond.substr(and_pos + 5));
        return check_complex_condition(table, row_index, left) && 
               check_complex_condition(table, row_index, right);
    }

    // Если есть скобки - обрабатываем вложенное выражение
    if (cond.front() == '(' && cond.back() == ')') {
        string inner_cond = cond.substr(1, cond.size() - 2);
        return check_complex_condition(table, row_index, inner_cond);
    }

    // Простое условие без операторов
    return check_simple_condition(table, row_index, cond);
}

void delete_data(const string& data_dir, const string& table_name, const string& condition) {
    wait_for_unlock(data_dir, table_name);

    Table* table = reinterpret_cast<Table*>(tables.get(table_name));
    if (!table) {
        unlock_table(data_dir, table_name);
        cout << "Table not found." << endl;
        return;
    }

    if (table->rows.size == 0) {
        unlock_table(data_dir, table_name);
        cout << "Table is empty. Nothing to delete." << endl;
        return;
    }

    CustVector<CustVector<string>> new_rows;
    bool any_match = false;
    size_t old_rows_size = table->rows.size;

    for (size_t i = 0; i < table->rows.size; ++i) {
        if (check_complex_condition(table, i, condition)) {
            any_match = true;
        } else {
            new_rows.push_back(table->rows[i]);
        }
    }

    if (!any_match) {
        unlock_table(data_dir, table_name);
        cout << "No rows matched the condition. Nothing to delete." << endl;
        return;
    }

    // Находим индекс первичного ключа
    size_t pk_index = 0;
    for (size_t i = 0; i < table->columns.size; ++i) {
        if (table->columns[i] == table->primary_key) {
            pk_index = i;
            break;
        }
    }

    // Пересчитываем значения первичного ключа
    for (size_t i = 0; i < new_rows.size; ++i) {
        new_rows[i][pk_index] = to_string(i + 1);
    }

    table->rows = new_rows;

    // Обновление последовательности первичных ключей
    write_pk_sequence(data_dir, table_name, new_rows.size);

    // Сохраняем в файл
    if (table->file_format == "json") {
        save_table_json(data_dir, *table);
    } else if (table->file_format == "csv") {
        save_table_csv(data_dir, *table);
    } else {
        save_table_json(data_dir, *table);
    }

    unlock_table(data_dir, table_name);
    cout << "Successfully deleted " << (old_rows_size - new_rows.size)
         << " rows from table '" << table_name << "'" << endl;
}

void select_data(const string& data_dir, const CustVector<string>& table_names, const CustVector<string>& columns, const string& condition = "") {
    // Проверка на количество таблиц
    if (table_names.size == 0) {
        cout << "No tables specified." << endl;
        return;
    }
    if (table_names.size > 2) {
        cout << "Error: Only 1 or 2 tables are allowed." << endl;
        return;
    }

    // Загружаем таблицы
    CustVector<const Table*> loaded_tables;
    for (size_t i = 0; i < table_names.size; ++i) {
        Table* table = reinterpret_cast<Table*>(tables.get(table_names[i]));
        if (!table) {
            load_table(data_dir, table_names[i]);
            table = reinterpret_cast<Table*>(tables.get(table_names[i]));
            if (!table) {
                cout << "Table not found: " << table_names[i] << endl;
                return;
            }
        }
        loaded_tables.push_back(table);
    }

    // Определяем колонки для вывода
    CustVector<string> selected_columns;
    if (table_names.size == 2 && !(columns.size == 1 && columns[0] == "*")) {
        cout << "Error: For 2 tables, only '*' is allowed between SELECT and FROM." << endl;
        return;
    }

    if (columns.size == 1 && columns[0] == "*") {
        for (size_t t = 0; t < loaded_tables.size; ++t) {
            const Table* table = loaded_tables[t];
            for (size_t j = 0; j < table->columns.size; ++j) {
                string full_col_name = table->name + "." + table->columns[j];
                selected_columns.push_back(full_col_name);
            }
        }
    } else {
        selected_columns = columns;
    }

    // Если две таблицы, проверяем условие
    if (table_names.size == 2) {
        if (condition.empty()) {
            cout << "Error: Condition is required for 2 tables." << endl;
            return;
        }

        // Проверяем формат условия
        size_t equal_pos = condition.find('=');
        if (equal_pos == string::npos) {
            cout << "Error: Condition must be in format 'first_value = second_value'." << endl;
            return;
        }

        string left_part = condition.substr(0, equal_pos);
        string right_part = condition.substr(equal_pos + 1);

        // Убираем пробелы
        left_part = trim(left_part);
        right_part = trim(right_part);

        // Проверяем наличие точек
        size_t left_dot_pos = left_part.find('.');
        size_t right_dot_pos = right_part.find('.');
        if (left_dot_pos == string::npos || right_dot_pos == string::npos) {
            cout << "Error: Condition must include table names (e.g., 'table1.column1 = table2.column2')." << endl;
            return;
        }

        string left_table = left_part.substr(0, left_dot_pos);
        string left_column = left_part.substr(left_dot_pos + 1);
        string right_table = right_part.substr(0, right_dot_pos);
        string right_column = right_part.substr(right_dot_pos + 1);

        // Проверяем, что первое значение принадлежит первой таблице, а второе - второй таблице
        if (left_table != loaded_tables[0]->name || right_table != loaded_tables[1]->name) {
            cout << "Error: First value in condition must belong to the first table and second value to the second table." << endl;
            return;
        }

        // Проверяем наличие колонок в таблицах
        int left_col_index = -1;
        int right_col_index = -1;
        for (size_t k = 0; k < loaded_tables[0]->columns.size; ++k) {
            if (loaded_tables[0]->columns[k] == left_column) {
                left_col_index = k;
                break;
            }
        }
        for (size_t k = 0; k < loaded_tables[1]->columns.size; ++k) {
            if (loaded_tables[1]->columns[k] == right_column) {
                right_col_index = k;
                break;
            }
        }

        if (left_col_index == -1 || right_col_index == -1) {
            cout << "Error: Columns in condition not found in tables." << endl;
            return;
        }

        // Выводим заголовки
        for (size_t i = 0; i < selected_columns.size; ++i) {
            size_t dot_pos = selected_columns[i].find(".");
            string column_name_part = (dot_pos != string::npos) ? selected_columns[i].substr(dot_pos + 1) : selected_columns[i];
            cout << column_name_part << "\t";
        }
        cout << endl;
        cout << string(selected_columns.size * 10, '-') << endl;

        // Ищем совпадающие строки
        for (size_t i = 0; i < loaded_tables[0]->rows.size; ++i) {
            for (size_t j = 0; j < loaded_tables[1]->rows.size; ++j) {
                if (loaded_tables[0]->rows[i][left_col_index] == loaded_tables[1]->rows[j][right_col_index]) {
                    // Выводим данные из первой таблицы
                    for (size_t col_idx = 0; col_idx < selected_columns.size; ++col_idx) {
                        string full_col_name = selected_columns[col_idx];
                        size_t dot_pos = full_col_name.find(".");
                        string table_name_part = full_col_name.substr(0, dot_pos);
                        string column_name_part = full_col_name.substr(dot_pos + 1);
                        if (table_name_part == loaded_tables[0]->name) {
                            for (size_t k = 0; k < loaded_tables[0]->columns.size; ++k) {
                                if (loaded_tables[0]->columns[k] == column_name_part) {
                                    cout << loaded_tables[0]->rows[i][k] << "\t";
                                    break;
                                }
                            }
                        }
                    }
                    // Выводим данные из второй таблицы
                    for (size_t col_idx = 0; col_idx < selected_columns.size; ++col_idx) {
                        string full_col_name = selected_columns[col_idx];
                        size_t dot_pos = full_col_name.find(".");
                        string table_name_part = full_col_name.substr(0, dot_pos);
                        string column_name_part = full_col_name.substr(dot_pos + 1);
                        if (table_name_part == loaded_tables[1]->name) {
                            for (size_t k = 0; k < loaded_tables[1]->columns.size; ++k) {
                                if (loaded_tables[1]->columns[k] == column_name_part) {
                                    cout << loaded_tables[1]->rows[j][k] << "\t";
                                    break;
                                }
                            }
                        }
                    }
                    cout << endl;
                }
            }
        }
    } else {
        // Если одна таблица
        const Table* table = loaded_tables[0];

        // Выводим заголовки
        for (size_t i = 0; i < selected_columns.size; ++i) {
            size_t dot_pos = selected_columns[i].find(".");
            string column_name_part = (dot_pos != string::npos) ? selected_columns[i].substr(dot_pos + 1) : selected_columns[i];
            cout << column_name_part << "\t";
        }
        cout << endl;
        cout << string(selected_columns.size * 10, '-') << endl;

        // Проходим по строкам таблицы и проверяем условие
        bool has_condition = !condition.empty();
        for (size_t i = 0; i < table->rows.size; ++i) {
            if (!has_condition || check_complex_condition(table, i, condition)) {
                for (size_t j = 0; j < selected_columns.size; ++j) {
                    string column_name_part = selected_columns[j];
                    size_t dot_pos = column_name_part.find(".");
                    if (dot_pos != string::npos) {
                        column_name_part = column_name_part.substr(dot_pos + 1);
                    }
                    bool value_found = false;
                    for (size_t k = 0; k < table->columns.size; ++k) {
                        if (table->columns[k] == column_name_part) {
                            cout << table->rows[i][k] << "\t";
                            value_found = true;
                            break;
                        }
                    }
                    if (!value_found) {
                        cout << "NULL\t";
                    }
                }
                cout << endl;
            }
        }
    }
}





// Функция для парсинга команд
CustVector<string> parse_command(const string& command) {
    CustVector<string> tokens;
    istringstream iss(command);
    string token;
    bool inside_quotes = false;
    string current_token = "";

    while (iss >> token) {
        if (token.front() == '(' && token.back() == ')') {
            tokens.push_back(token.substr(1, token.size() - 2));
        }
        else if (token.front() == '(') {
            inside_quotes = true;
            current_token += token.substr(1) + " ";
        }
        else if (token.back() == ')') {
            inside_quotes = false;
            current_token += token.substr(0, token.size() - 1);
            tokens.push_back(current_token);
            current_token = "";
        }
        else if (inside_quotes) {
            current_token += token + " ";
        }
        else {
            tokens.push_back(token);
        }
    }

    if (!current_token.empty()) {
        tokens.push_back(current_token);
    }

    return tokens;
}

int main(int argc, char* argv[]) {
    // Проверка формата команды
    if (argc != 5 || string(argv[1]) != "--file" || string(argv[3]) != "--query") {
        cerr << "Usage: " << argv[0] << " --file <data_directory> --query '<SQL_command>'" << endl;
        return 1;
    }

    // Получаем директорию с данными
    string data_dir = argv[2];
    
    // Проверяем существование директории
    if (!fs::exists(data_dir) || !fs::is_directory(data_dir)) {
        cerr << "Error: Directory not found: " << data_dir << endl;
        return 1;
    }

    // Разбираем query часть
    string query(argv[4]);
    CustVector<string> tokens = parse_command(query);
    
    if (tokens.size == 0) {
        cerr << "Error: Empty query" << endl;
        return 1;
    }

    string command = tokens[0];

    try {
    if (command == "SELECT") {
    if (tokens.size < 4) {
        cerr << "Invalid SELECT command. Usage: SELECT column1,column2 FROM table_name1,table_name2 [WHERE condition]" << endl;
        return 1;
    }

    // Ищем позицию FROM
    size_t from_pos = 0;
    for (size_t i = 1; i < tokens.size; ++i) {
        if (tokens[i] == "FROM") {
            from_pos = i;
            break;
        }
    }

    if (from_pos == 0) {
        cerr << "Missing FROM keyword in SELECT command" << endl;
        return 1;
    }

    // Проверяем, что между SELECT и FROM есть колонки
    bool has_columns = false;
    for (size_t i = 1; i < from_pos; ++i) {
        if (!tokens[i].empty() && tokens[i] != ",") {
            has_columns = true;
            break;
        }
    }

    if (!has_columns) {
        cerr << "Error: No columns specified between SELECT and FROM" << endl;
        return 1;
    }

    // Получаем колонки (все токены между SELECT и FROM)
    CustVector<string> columns;
    for (size_t i = 1; i < from_pos; ++i) {
        string column_str = tokens[i];
        if (column_str.find(',') != string::npos) {
            istringstream iss_columns(column_str);
            string column;
            while (getline(iss_columns, column, ',')) {
                string trimmed_column = trim(column);
                if (!trimmed_column.empty()) {
                    columns.push_back(trimmed_column);
                }
            }
        } else {
            string trimmed_column = trim(column_str);
            if (!trimmed_column.empty()) {
                columns.push_back(trimmed_column);
            }
        }
    }

    // Проверяем, что колонки были успешно извлечены
    if (columns.size == 0) {
        cerr << "Error: No valid columns specified between SELECT and FROM" << endl;
        return 1;
    }

    // Ищем позицию WHERE (если есть)
    size_t where_pos = tokens.size;
    for (size_t i = from_pos + 1; i < tokens.size; ++i) {
        if (tokens[i] == "WHERE") {
            where_pos = i;
            break;
        }
    }

    // Получаем имена таблиц (между FROM и WHERE или концом)
    CustVector<string> table_names;
    size_t table_start = from_pos + 1;
    size_t table_end = (where_pos < tokens.size) ? where_pos : tokens.size;
    for (size_t i = table_start; i < table_end; ++i) {
        string table_str = tokens[i];
        if (table_str.find(',') != string::npos) {
            istringstream iss_tables(table_str);
            string table_name;
            while (getline(iss_tables, table_name, ',')) {
                string trimmed_table_name = trim(table_name);
                if (!trimmed_table_name.empty()) {
                    table_names.push_back(trimmed_table_name);
                }
            }
        } else {
            string trimmed_table_name = trim(table_str);
            if (!trimmed_table_name.empty()) {
                table_names.push_back(trimmed_table_name);
            }
        }
    }

    // Проверяем, что хотя бы одна таблица указана
    if (table_names.size == 0) {
        cerr << "Error: No tables specified after FROM" << endl;
        return 1;
    }

    // Получаем условие WHERE (если есть)
    string condition = "";
    if (where_pos < tokens.size - 1) {
        for (size_t i = where_pos + 1; i < tokens.size; ++i) {
            if (i > where_pos + 1) condition += " ";
            condition += tokens[i];
        }
        if (condition.empty()) {
            cerr << "Error: WHERE condition is empty" << endl;
            return 1;
        }
    }

    // Загружаем все указанные таблицы и проверяем успешность загрузки
    for (size_t i = 0; i < table_names.size; ++i) {
        load_table(data_dir, table_names[i]);
        Table* table = reinterpret_cast<Table*>(tables.get(table_names[i]));
        if (!table) {
            return 1;
        }
    }

    select_data(data_dir, table_names, columns, condition);
}
    else if (command == "INSERT") {
    if (tokens.size < 4 || tokens[1] != "INTO" || tokens[3] != "VALUES") {
        cerr << "Invalid INSERT command. Usage: INSERT INTO table_name VALUES value1,value2" << endl;
        return 1;
    }

    string table_name = tokens[2];
    load_table(data_dir, table_name); // Загружаем таблицу

    // Проверяем что таблица загрузилась
    Table* table = reinterpret_cast<Table*>(tables.get(table_name));
    if (!table) {
        return 1;
    }

    // Дополнительная проверка что таблица корректно инициализирована
    if (table->columns.size == 0) {
        cerr << "Error: Table '" << table_name << "' has no columns" << endl;
        return 1;
    }

    CustVector<string> values;
    for (size_t i = 4; i < tokens.size; ++i) {
        values.push_back(trim(tokens[i]));
    }

    // Проверяем количество значений
    if (values.size != table->columns.size - 1) {
        cerr << "Error: Expected " << table->columns.size - 1 << " values, but got " << values.size << endl;
        cerr << "Table columns: ";
        for (size_t i = 1; i < table->columns.size; ++i) {
            cerr << table->columns[i];
            if (i < table->columns.size - 1) cerr << ", ";
        }
        cerr << endl;
        return 1;
    }

    insert_data(data_dir, table_name, values);
}
    else if (command == "DELETE") {
        if (tokens.size < 4 || tokens[1] != "FROM" || tokens[3] != "WHERE") {
            cerr << "Invalid DELETE command. Usage: DELETE FROM table_name WHERE condition" << endl;
            return 1;
        }
        
        string table_name = tokens[2];
        load_table(data_dir, table_name);
        
        string condition;
        for (size_t i = 4; i < tokens.size; ++i) {
            if (i > 4) condition += " ";
            condition += tokens[i];
        }
        
        delete_data(data_dir, table_name, condition);
    }
    else if (command == "CREATE") {
        if (tokens.size < 6 || tokens[1] != "TABLE" || tokens[tokens.size - 2] != "PRIMARY_KEY") {
            cerr << "Invalid CREATE TABLE command. Usage: CREATE TABLE table_name (column1,column2) PRIMARY_KEY primary_key" << endl;
            return 1;
        }
        
        string table_name = tokens[2];
        CustVector<string> columns;
        
        // Парсим колонки (пропускаем скобки и запятые)
        for (size_t i = 3; i < tokens.size; ++i) {
            if (tokens[i] == "(" || tokens[i] == ")" || tokens[i] == ",") continue;
            if (tokens[i] == "PRIMARY_KEY") break;
            columns.push_back(trim(tokens[i]));
        }
        
        // Ищем PRIMARY_KEY
        string primary_key;
        for (size_t i = 3; i < tokens.size; ++i) {
            if (tokens[i] == "PRIMARY_KEY" && i + 1 < tokens.size) {
                primary_key = tokens[i + 1];
                break;
            }
        }
        
        create_table(data_dir, table_name, columns, primary_key);
    }
    else if (command == "SAVE") {
    if (tokens.size == 3) {
        string format = tokens[1];
        string table_name = tokens[2];
        
        Table* table = reinterpret_cast<Table*>(tables.get(table_name));

        // Загружаем таблицу чтобы получить актуальные данные
        load_table(data_dir, table_name);
        table = reinterpret_cast<Table*>(tables.get(table_name));

        if (format == "CSV") {
            save_as_csv(data_dir, table_name, *table);
        } else if (format == "JSON") {
            save_as_json(data_dir, table_name, *table);
        } else {
            cerr << "Invalid format: " << format << ". Use CSV or JSON" << endl;
            return 1;
        }
    } else {
        cerr << "Invalid SAVE command. Usage: SAVE CSV table_name or SAVE JSON table_name" << endl;
        return 1;
    }
}
    else {
        cerr << "Unknown command: " << command << endl;
        return 1;
    }
}
catch (const exception& e) {
    cerr << "Error: " << e.what() << endl;
    return 1;
}
}
