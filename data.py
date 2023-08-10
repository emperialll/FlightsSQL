"""
Module: SQL Database Connection

This module provides a function to create a SQLAlchemy database engine
and import the 'text' class for executing raw SQL queries.

Functions:
    create_database_engine(url: str) -> sqlalchemy.engine.base.Engine
        Creates and returns a SQLAlchemy database engine based on the provided URL.

Imported Classes:
    text
        A class from the 'sqlalchemy' module that is used to define and execute
        raw SQL queries within SQLAlchemy.

Example:
    engine = create_database_engine("sqlite:///mydatabase.db")
    query = text("SELECT * FROM users WHERE age > :age")
    result = engine.execute(query, age=25)
    for row in result:
        print(row)
"""
from sqlalchemy import create_engine, text

QUERY_FLIGHT_BY_ID = "SELECT flights.*, airlines.airline, flights.ID as FLIGHT_ID, \
    flights.DEPARTURE_DELAY as DELAY FROM flights \
        JOIN airlines ON flights.airline = airlines.id \
            WHERE flights.ID = :id"
QUERY_FLIGHT_BY_DATE = "SELECT flights.*, flights.ID as FLIGHT_ID, \
    flights.ORIGIN_AIRPORT, flights.DESTINATION_AIRPORT, flights.DEPARTURE_DELAY as DELAY \
        FROM flights WHERE flights.DAY = :day \
            AND flights.MONTH = :month AND flights.YEAR = :year"
QUERY_FLIGHT_BY_AIRPORT = "SELECT flights.*, flights.ID as FLIGHT_ID, \
    flights.ORIGIN_AIRPORT, flights.DESTINATION_AIRPORT, airlines.AIRLINE, flights.DEPARTURE_DELAY as DELAY \
        FROM flights JOIN airlines ON flights.AIRLINE = airlines.ID \
            WHERE flights.ORIGIN_AIRPORT = :origin_airport"
QUERY_FLIGHT_BY_AIRLINE = "SELECT airlines.*, airlines.ID as AIRLINE_ID, \
    flights.ORIGIN_AIRPORT, flights.DESTINATION_AIRPORT, airlines.AIRLINE, flights.DEPARTURE_DELAY as DELAY \
        FROM flights JOIN airlines ON  airlines.ID = flights.AIRLINE \
            WHERE airlines.AIRLINE = :airline"

class FlightData:
    """
    The FlightData class is a Data Access Layer (DAL) object that provides an
    interface to the flight data in the SQLITE database. When the object is created,
    the class forms connection to the sqlite database file, which remains active
    until the object is destroyed.
    """
    def __init__(self, db_uri):
        """
        Initialize a new engine using the given database URI
        """
        self._engine = create_engine(db_uri)


    def _execute_query(self, query, params):
        """
        Execute an SQL query with the params provided in a dictionary,
        and returns a list of records (dictionary-like objects).
        If an exception was raised, print the error, and return an empty list.
        """
        records = []
        with self._engine.connect() as conn:
            # Use SQLAlchemy's text() object to bind parameters and execute the query
            result = conn.execute(text(query), params)
            for row in result:
                records.append(row._asdict())
            return records

    def get_delayed_flights_by_airline(self, origin_airport):
        """
        Searches for flights using airline name.
        If the flight was found, return a list with a single record
        """
        params = {'airline': origin_airport}
        return self._execute_query(QUERY_FLIGHT_BY_AIRLINE, params)

    def get_delayed_flights_by_airport(self, origin_airport):
        """
        Searches for flights using origin airport IATA code.
        If the flight was found, return a list with a single record
        """
        params = {'origin_airport': origin_airport}
        return self._execute_query(QUERY_FLIGHT_BY_AIRPORT, params)

    def get_flight_by_id(self, flight_id):
        """
        Searches for flight details using flight ID.
        If the flight was found, returns a list with a single record.
        """
        params = {'id': flight_id}
        return self._execute_query(QUERY_FLIGHT_BY_ID, params)

    def get_flights_by_date(self, day, month, year):
        """
        Searches for flight details using Date.
        If the flight(s) was/were found, returns a list with a single record.
        """
        params = {'day': day, 'month': month, 'year': year}
        return self._execute_query(QUERY_FLIGHT_BY_DATE, params)

    def __del__(self):
        """
        Closes the connection to the databse when the object is about to be destroyed
        """
        self._engine.dispose()
    