from fastapi import FastAPI, HTTPException
from extra_methods import router_extra
from necessary_methods import router


app = FastAPI()


app.include_router(router_extra)
app.include_router(router)