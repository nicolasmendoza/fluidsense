from datetime import datetime

from fastapi import FastAPI
from pydantic import BaseModel, field_validator
import uvicorn
from sensor_lib import get_sensor_dataset


class SensorData(BaseModel):
    """
    Defines the sensor data model, including field validations.
    This is very useful to maintain consistency in our dataframe when
    new data arrived.
    """
    Fecha: str
    Hora: str
    Sensor: str
    Medicion: float
    Estado: str

    @field_validator('Fecha')
    def validate_date_format(cls, value: str):
        """Validates the date is in the correct 'YYYY-MM-DD' format."""
        try:
            datetime.strptime(value, '%Y-%m-%d')
        except ValueError:
            raise ValueError("Fecha debe estar en formato 'YYYY-MM-DD'")
        return value

    @field_validator('Hora')
    def validate_time_format(cls, value):
        """Validates the time is in the correct 'HH:MM:SS' format."""
        try:
            datetime.strptime(value, '%H:%M:%S')
        except ValueError:
            raise ValueError("Hora debe estar en formato 'HH:MM:SS'")
        return value

    @field_validator('Estado')
    def validate_status(cls, value):
        """Validates the status is either 'NORMAL' or 'RECOVERING'."""
        if value not in ['NORMAL', 'RECOVERING']:
            raise ValueError("Estado debe ser 'NORMAL' o 'RECOVERING'")
        return value


app = FastAPI()
sensor_data = get_sensor_dataset()


@app.get("/api/sensors", summary="Retrieve Sensor Data")
def get_sensors():
    return sensor_data.get_data()


@app.post("/api/sensors/upload", summary="Upload Sensor Data")
def add_sensors(data: list[SensorData]):
    sensor_data.add_data(data)
    return {"message": "Sensor data was successfully added"}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
