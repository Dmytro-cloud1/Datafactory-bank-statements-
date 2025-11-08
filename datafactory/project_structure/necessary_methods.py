from fastapi import APIRouter, HTTPException, UploadFile
from structure_of_db import  Credits, session, Plans, Dictionary
from datetime import date
import pandas as pd


router = APIRouter(prefix="/main")

@router.get('/user_credits/{user_id}')
def get_user_credits(user_id: int):
    credits = session.query(Credits).filter(Credits.id == user_id).all()

    if not Credits:
        raise HTTPException(status_code=404, detail= "No credits or user not found")
    
    result = []
    today = date.today()

    for credit in credits:
        credit_data = {
            "issuance_date" : credit.issuance_date,
            "is_closed": credit.actual_return_date is None
        }
        #Для закритых кредитов:
        if credit_data["is_closed"]:
            total_sum = sum(p.sum for p in credit.payments) if credit.payments else 0
            credit_data.update({
                "return_date" : credit.actual_return_date,
                "issue_amount" : credit.body,
                "percents" : credit.percent,
                "payments_sum": total_sum
            })

        #Для открытых кредитов:
        else:
            overdue_time = (today - credit.return_date).days if credit.return_date < today else 0
            body_payment = sum(b_p.sum for b_p in credit.payments) if credit.payments else 0
            percents_payment = 0

            credit_data.update({
                "return_date": credit.return_date,
                "overdue_time": overdue_time,
                "issue_amount": credit.body,
                "percents": credit.percent,
                "body_payment": body_payment,
                "percents_payment": percents_payment
            })

        result.append(credit_data)

    return result

@router.post("/plans_insert")
def plans_insert(exel_file: UploadFile):
    df = pd.read_excel(exel_file.file)

    #перевірка на наявність у БД плану з місяцем 
    required_columns = ['period', 'category', 'sum']
    for col in required_columns:
        if col not in df.columns:
            raise HTTPException(status_code= 402, detail= "No columns:{col}")
    
    #Метод має перевіряти на правильність заповнення місяця плану (має бути вказано перше число місяця).
    for idx, date_value in enumerate(df['period'], start = 1):
        try:
            date = pd.to_datetime(date_value)
            if date.day != 1:
                raise HTTPException(status_code=404, 
                            detail= f"Doesn't correct date in {idx}: must be firts day in month")
    
        except:
            raise HTTPException(status_code=404, 
                            detail= f"Doesn't correct format in {idx}: {date_value}")
        
    #перевірка суми на пусті значення
    for idx1, sum_value in enumerate(df['sum'], start= 1):
        if pd.isna(sum_value):
            raise HTTPException(status_code= 404, detail= f'Value is empty in: {idx1}')

    result = []

    for _, d_q in df.iterrows():
        try:
            dict_query = session.query(Dictionary).filter(
                Dictionary.name == d_q["category"]).first()
            if not dict_query:
                raise HTTPException(status_code= 404, detail= "We don't have category_name. Doesn't exist")
            
            plans = Plans(period = d_q["period"], sum = d_q["sum"], category_id = dict_query.id)
    
            result.append(plans)
        
        except Exception as ex:
            print(f"Error:{ex}")


    # session.add_all(result)
    # session.commit()
    return {"message":"All dates - first day of month"}
    