import os
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, Date, Float
from sqlalchemy.orm import Session, declarative_base, relationship
from dotenv import load_dotenv

load_dotenv()
        
host= os.getenv("host")
port= os.getenv("port")
password= os.getenv("password")
user = os.getenv("user")
database= os.getenv("db_name")


url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
engine = create_engine(url) 
Base = declarative_base()


class Users(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key= True, autoincrement= True)
    login = Column(String(30))
    registration_date = Column(Date)


class Credits(Base):
    __tablename__ = 'credits'
    id = Column(Integer, primary_key=True, autoincrement= True)
    user_id = Column(Integer, ForeignKey('users.id'))
    issuance_date = Column(Date)
    return_date = Column(Date)
    actual_return_date = Column(Date, nullable=True)
    body = Column(Integer)
    percent = Column(Float)

    connect_user = relationship("Users", backref= 'credits')


class Dictionary(Base):
    __tablename__ = 'dictionary'
    id = Column(Integer, primary_key= True, autoincrement= True)
    name = Column(String(30))


class Plans(Base):
    __tablename__ = 'plans'
    id = Column(Integer, primary_key=True, autoincrement= True)
    period = Column(Date)
    sum = Column(Integer)    
    category_id  = Column(Integer, ForeignKey('dictionary.id'))

    connect_dictionary = relationship(Dictionary, backref= 'plans')


class Payments(Base):
    __tablename__ = "payments"
    id = Column(Integer, primary_key= True, autoincrement= True)
    sum = Column(Integer)
    payment_date = Column(Date)
    credit_id = Column(Integer, ForeignKey('credits.id'))
    type_id = Column(Integer, ForeignKey('dictionary.id'))

    connect_credits = relationship(Credits, backref= 'payments')
    connect_type = relationship(Dictionary, backref='payments')


Base.metadata.create_all(engine)

session = Session(engine)


# Загрузка данных с csv в DB

# ПУТИ К ФАЙЛАМ
BASE_DIR = os.path.dirname(__file__)
CSV_DIR = os.path.join(os.path.dirname(BASE_DIR), "csv_files")

users_csv = os.path.join(CSV_DIR, "users.csv")
credits_csv = os.path.join(CSV_DIR, "credits.csv")
dictionary_csv = os.path.join(CSV_DIR, "dictionary.csv")
plans_csv = os.path.join(CSV_DIR, "plans.csv")
payments_csv = os.path.join(CSV_DIR, "payments.csv")

def parse_date(x: str):
    """
    pandas.isna(x) проверяет, является ли значение "пустым" по pandas.
    Сюда относятся:
    1.NaN (Not a Number) — пустые числовые значения в DataFrame
    2.None — пустые объекты Python

    str(x).strip() == '' --- На всякий случай превращаем x в строку и убираем пробелы в начале и конце.  
    """

    if pd.isna(x) or x == '' or str(x).strip() == '':
        return None
    try:
        return datetime.strptime(x, "%d.%m.%Y").date() # пробует распарсить строку в формате "день.месяц.год"
    except ValueError:
        return None

def load_csv_to_db(session: Session, model, csv_file: str, delimiter='\t'):
    df = pd.read_csv(csv_file, delimiter=delimiter, encoding='utf-8')

    for col in df.columns:
        if "date" in col.lower():
            df[col] = df[col].apply(parse_date) #обрабатываются все даты сразу (issuance_date, return_date, actual_return_date)

    df = df.where(pd.notnull(df), None) #заменяет все NaN на None.
    
    #orient="records" превращает таблицу DataFrame в список строк, 
    # где каждая строка — это словарь с колонками как ключи.
    objects = [model(**row) for row in df.to_dict(orient="records")] 
    
    # session.add_all(objects)
    # session.commit()
    print(f"✅ Загружено {len(objects)} записей из {csv_file}")


load_csv_to_db(session, Users, users_csv, delimiter='\t')
load_csv_to_db(session, Credits, credits_csv, delimiter='\t')
load_csv_to_db(session, Dictionary, dictionary_csv, delimiter='\t')
load_csv_to_db(session, Plans, plans_csv, delimiter='\t')
load_csv_to_db(session, Payments, payments_csv, delimiter='\t')