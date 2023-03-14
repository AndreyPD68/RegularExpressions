import psycopg2



# удаление таблиц для новой работы
def delete_tables(cur):
    cur.execute("""
    DROP TABLE IF EXISTS Client_phones""")
    cur.execute("""
    DROP TABLE  IF EXISTS Clients""")
    conn.commit()

# Создаю таблицы
def create_db(cur):
    cur.execute("""
    CREATE TABLE IF NOT EXISTS Clients(  
        client_id INTEGER PRIMARY KEY,
        last_name VARCHAR(40) ,
        first_name VARCHAR(40),
        email VARCHAR(60)
    );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS Client_phones(  
            phone_number  VARCHAR PRIMARY KEY,
            client_id INTEGER REFERENCES Clients(client_id)
        );
        """)
    conn.commit()

# Добавляю данные о клиенте
def add_client(cur, client_id, first_name, last_name, email, *phones):
    cur.execute("""
        INSERT INTO Clients (client_id, last_name, first_name, email) VALUES(%s, %s, %s,%s)
        RETURNING email, last_name, first_name;
            """, (client_id,  last_name, first_name, email))
    print(f'Клиент №{client_id} - , {cur.fetchall()}')

    if phones:
        add_phone(cur, client_id, *phones)


def add_phone(cur, client_id, *phones):
    for phone in phones:
        cur.execute("""
            INSERT INTO Client_phones (phone_number, client_id) VALUES(%s, %s) 
            RETURNING phone_number, client_id;
            """, (phone, client_id))
        print(f'Номер Телефона клиента с id: {client_id} - {cur.fetchall()}')

# смена данных
def change_client_data(cur, client_id, first_name=None, last_name=None, email=None,
                       new_phones=None, old_phones=None):
    print('Обновляю данные')
    if first_name:
        cur.execute("""
                    UPDATE Clients SET first_name=%s WHERE client_id=%s;""", (first_name, client_id))
        conn.commit()
    if last_name:
        cur.execute("""
                    UPDATE Clients SET first_name=%s WHERE client_id=%s;""", (last_name, client_id))
        conn.commit()
    if email:
        cur.execute("""
                    UPDATE Clients SET first_name=%s WHERE client_id=%s;""", (email, client_id))
        conn.commit()
    cur.execute("""
                    SELECT last_name, first_name, email FROM Clients WHERE client_id=%s
                    ;
                    """, (client_id,))

    print(f'Смена данных id: {client_id}, новые данные {cur.fetchall()}')
    if old_phones:
        for old_phone in old_phones:
            print('Удаление старых номеров телефона')
            delete_phone(cur, old_phone)

    if new_phones:
        print('Добавление новых номеров телефона')
        add_phone(cur, client_id, *new_phones)

def delete_phone(cur, phone_number):
    cur.execute("""
        DELETE FROM Client_phones WHERE phone_number=%s
        RETURNING phone_number, client_id;
        """, (phone_number,))
    print(f'Удаление телефона №{phone_number}  - {cur.fetchone()}')


def delele_client(cur, id):
    cur.execute("""
        DELETE FROM Client_phones WHERE client_id=%s 
        RETURNING client_id, phone_number;
        """, (id,))
    print(f'Удаление данных клиента №{id} - {cur.fetchall()}')

    cur.execute("""
        DELETE FROM Clients WHERE client_id=%s 
        ;
        """, (id,))
    conn.commit()
    print(f'Удаление всех данных клиента №{id}')

# Поиск клиента по данным
def find_client(cur, client_id=None,  first_name=None, last_name=None, email=None, phone_number=None):
    if phone_number:    #Проверяем был ли введен телефон
        cur.execute("""
                SELECT last_name FROM Clients 
                JOIN Client_phones ON clients.client_id=client_phones.client_id
                WHERE client_phones.phone_number=%s
                ;
                """, (phone_number,))
        print(f'Поиск клиента по номеру телефона {phone_number} - {cur.fetchall()}')

    else:
        cur.execute("""
            SELECT last_name FROM Clients 
            WHERE client_id=%s OR first_name=%s OR last_name=%s OR email=%s
            ;
            """, (client_id,  first_name, last_name, email))
        print(f'Поиск клиента по другим параметрам  - {cur.fetchall()}')


with psycopg2.connect(database="Home_Work", user="postgres", password="***") as conn:
    with conn.cursor() as cur:
        delete_tables(cur)
        create_db(cur)
        add_client(cur, 1, 'Иван', 'Иванов', 'Ivanvano@mail.ru', '+79227776666', '+11233211496')  #Добавляем клиента с телефонами
        add_client(cur, 2, 'Геннадий', 'Букин', 'genabuk@gmail.com')  #Добавляем клиента без телефонов
        add_client(cur, 3, 'Олег', 'Шматко', 'Corporal@yandex.ru', '+777-777')
        add_phone(cur, 2, '+71112223344')  #Добаляем телефон к отдельному клиенту
        change_client_data(cur, client_id=3, first_name='Олег', old_phones=['+777-777'],
                           new_phones=['+666-666', '+888-888'])
        delete_phone(cur, '+777-777')
        delele_client(cur, 1)
        find_client(cur, last_name='Букин')
        find_client(cur, first_name='Иван') # клиент удален
        find_client(cur, phone_number='+71112223344')
        find_client(cur, phone_number='+777-777')  # номер удален
        find_client(cur, client_id=3)

conn.close()
