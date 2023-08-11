
from fastapi import FastAPI,HTTPException
from sqlalchemy import create_engine,text,Table, Column, Integer, Float, String,MetaData
from sqlalchemy.orm import sessionmaker

# Create an instance of the FastAPI app
app = FastAPI()

# Configure the database connection
DATABASE_URL = "mysql://root:sql2022#@localhost/task_3"  # Replace with your MySQL connection details

# Create the database engine
engine = create_engine(DATABASE_URL)


# Create a SessionLocal class
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

#To get names of flights from particular airport
@app.get("/airport{airport_id}")
def get_flights(airport_id:int):
    db = SessionLocal()
    raw_query = text("SELECT flight_name FROM flight WHERE arrival=:arrival OR departure=:departure")
    result=db.execute(raw_query,{"arrival": airport_id, "departure": airport_id})
    flights = [{"Flight": row[0]} for row in result]
    return flights


#To get the busiest airport
@app.get("/busiest{time}")
def get_busiest_airport(time:str):
    db = SessionLocal()
    raw_query1 = text("SELECT arrival FROM flight WHERE LEFT(arrival_time, 2)=:time")
    result1=db.execute(raw_query1,{"time": time})
    arrival=[]
    departure=[]
    for row in result1:
        {
            arrival.append(row[0])
        }
    raw_query2 = text("SELECT departure FROM flight WHERE LEFT(departure_time, 2)=:time")
    result2=db.execute(raw_query2,{"time": time})
    for row in result2:
        {
            departure.append(row[0])
        }
    airports=arrival+departure
    counter = 0
    busiest = airports[0]
    for airport in airports:
        curr_frequency = airports.count(airport)
        if(curr_frequency> counter):
            counter = curr_frequency
            busiest = airport
    raw_query3 = text("SELECT airport_name FROM airports WHERE airport_id=:busiest")
    result3=db.execute(raw_query3,{"busiest": busiest})
    Airport = [{"Airport": row[0]} for row in result3]
    return Airport
     
    
 


#To get the next flight
@app.get("/nextFlight{time,destination}")
def get_flights(time:str,destination:int):
    db = SessionLocal()
    raw_query = text("SELECT flight_name FROM flight WHERE departure_time>=:time AND arrival=:destination GROUP BY departure ORDER BY departure_time")
    result=db.execute(raw_query,{"time": time, "destination": destination})
    next_flights = [{"Flights": row[0]} for row in result]
    next_flight=next_flights[0]
    return next_flight

    

# Run the FastAPI application with uvicorn
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app,host="localhost",port=8000)