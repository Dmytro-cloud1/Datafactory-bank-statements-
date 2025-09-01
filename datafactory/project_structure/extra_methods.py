from fastapi import FastAPI, HTTPException
from structure_of_db import Payments, Plans, Credits, session
from datetime import datetime
from sqlalchemy import func, extract

app = FastAPI()

@app.get('/plans_performance/{date_str}')
def get_info_plans(date_str: str):
    try:
        day = datetime.strptime(date_str, "%d.%m.%Y").date()
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format")

    start_of_month = day.replace(day=1) #создаёт новую дату, в которой день заменён на 1, а год и месяц остаются такими же
    if start_of_month.month == 12:
        start_of_next_month = start_of_month.replace(year=start_of_month.year+1, month=1, day=1) # 2021-01-01
    else:
        start_of_next_month = start_of_month.replace(month=start_of_month.month+1, day=1) #месяц не декабрь то просто на +1(2020-04 -> 2020-05)

    plans = session.query(Plans).filter(Plans.period >= start_of_month, Plans.period < start_of_next_month).all()

    if not plans:
        raise HTTPException(status_code=404, detail="No plans for this period")

    result = []

    for p in plans:
        fact_sum = 0

        # Блок для категория выдача
        if p.connect_dictionary.name.lower() == "видача":
            credits = session.query(Credits).filter(
                Credits.issuance_date >= start_of_month,  # с начала месяца (например, 2025-08-01)
                Credits.issuance_date <= day             # до той даты, которую передал пользователь (например, 2025-08-28)
            ).all()
            fact_sum = sum(c.body for c in credits)

        # Блок для категории сбор по такому же принцыпу
        elif p.connect_dictionary.name.lower() == "збір":
            payments = session.query(Payments).filter(
                Payments.payment_date >= start_of_month,
                Payments.payment_date <= day              
            ).all()
            fact_sum = sum(pay.sum for pay in payments)

        percents = (fact_sum / p.sum * 100) if p.sum else 0

        result.append({
            "period": p.period.strftime("%Y-%m-%d"),     
            "category": p.connect_dictionary.name,       
            "sum_of_plan": p.sum,                        
            "fact_sum": fact_sum,                        
            "percents": percents
        })

    return result


@app.get("/year_performance/{year_str}")
def get_year_perfomance(year_str: str):
    try:
        year = datetime.strptime(year_str, "%m.%Y").year
    except:
        raise HTTPException(status_code= 404,  detail="Invalid date format")
    

    result = []
    
    #суми видач за рік
    total_issued_year =  session.query(func.sum(Credits.body)).filter(
            extract("year", Credits.issuance_date) == year).scalar() or 0
    
    #суми платежів за рік
    total_payments_year = session.query(func.sum(Payments.sum)).filter(
            extract("year", Payments.payment_date) == year).scalar() or 0

    for month in range(1,13):
        #Кількість видач за місяць
        count_credits = session.query(func.count(Credits.id)).filter(
            extract("month", Credits.issuance_date) == month, #все даты выдачи за какой-то определенный месяц
            extract("year",Credits.issuance_date) == year
        ).scalar() or 0 #заменить None на 0 чтобы не выдало TypeError при подсчете %

        #Сума з плану по видачам на місяць
        plan_sum_vydacha = session.query(func.sum(Plans.sum)).filter(
            extract("month", Plans.period) == month,
            extract("year", Plans.period) == year,
            Plans.connect_dictionary.has(name = "видача")
        ).scalar() or 0

        #Сума видач за місяць
        issues_sum = session.query(func.sum(Credits.body)).filter(
            extract("month", Credits.issuance_date) == month,
            extract("year", Credits.issuance_date) == year
        ).scalar() or 0

        #% виконання плану по видачам
        percents_vydacha = (issues_sum/plan_sum_vydacha * 100) if plan_sum_vydacha else 0

        #Кількість платежів за місяць
        count_payments = session.query(func.count(Payments.id)).filter(
            extract("month", Payments.payment_date) == month,
            extract("year", Payments.payment_date) == year
        ).scalar() or 0

        #Сума з плану по збору за місяць
        plan_sum_zbir = session.query(func.sum(Plans.sum)).filter(
            extract("month", Plans.period) == month,
            extract("year", Plans.period) == year,
            Plans.connect_dictionary.has(name = "збір")
        ).scalar() or 0

        #Сума платежів за місяць
        payments_sum = session.query(func.sum(Payments.sum)).filter(
            extract("month", Payments.payment_date) == month,
            extract("year", Payments.payment_date) == year
        ).scalar() or 0

        #% виконання плану по збору
        percents_zbir = (payments_sum/plan_sum_zbir*100) if plan_sum_zbir else 0
        
        # % суми видач за місяць від суми видач за рік 
        percents_year_vydacha = (issues_sum/total_issued_year *100) if total_issued_year else 0

        # % суми платежів за місяць від суми платежів за рік
        percents_year_payments = (payments_sum/total_payments_year *100) if total_payments_year else 0

        result.append({
            "month_year": f"{month:02d}.{year}",
            "count_credits": count_credits,
            "plan_sum_vydacha": plan_sum_vydacha,
            "issues_sum": issues_sum,
            "percents_vydacha": percents_vydacha,
            "count_payments": count_payments,
            "plan_sum_zbir":plan_sum_zbir,
            "payments_sum":  payments_sum,
            "percents_zbir": percents_zbir,
            "percents_year_vydacha": percents_year_vydacha,
            "percents_year_payments": percents_year_payments 
        })
    
    return result