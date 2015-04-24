from metadata import get_Fluksos
Fluksos = get_Fluksos()

for flukso in Fluksos:
    print flukso.flukso_id, flukso.address
    for sensor in flukso.sensors:
        print sensor.sensor_id, sensor.sensortype, sensor.function
    print "\n"