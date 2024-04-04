'''
Created on 2024(e)ko api. 4(a)

@author: sergoras
'''
import mariadb
import sys
from PyP100 import PyP100
from _datetime import timedelta, datetime

if __name__ == '__main__':
    # Connect to MariaDB Platform
    try:
        conn = mariadb.connect(
            user="BeerFermContApp",
            password="456bFcA789#%",
            host="192.168.0.14",
            port=3306,
            database="BeerDB"
        )
    except mariadb.Error as e:
        print(f"Error connecting to MariaDB Platform: {e}")
        sys.exit(1)
    
    # Get Cursor and execute select
    cur = conn.cursor()
    cur.execute("SELECT MOMENT, HYDROM_NAME, GRAVITY, TEMP FROM READING ORDER BY MOMENT DESC FETCH FIRST 1 ROWS ONLY")
    lecturas = cur.fetchall()
    moment, hydromname, gravity, temp = lecturas[0][0], lecturas[0][1], lecturas[0][2], lecturas[0][3]
    now = datetime.utcnow()
    if (now - datetime.utcfromtimestamp(moment)) < timedelta(minutes=10):
        # Hay una lectura de hace menos de 10 minutos
        cur.execute("SELECT CONFIG_ID FROM HYDROM WHERE NAME = ?", (hydromname,))
        hydroms = cur.fetchall()
        for hydrom in hydroms:
            # Analizamos cada configuración con ese nombre de hydrom
            configid = hydrom[0]
            cur.execute("SELECT TOLERANCE, START_DATE, END_DATE FROM CONFIG WHERE T1.ID = ? ", (configid,))
            config = cur.fetchall()[0]
            tolerance, startdate, enddate,= config[0], config[1], config[2]
            if datetime.utcfromtimestamp(startdate) < now and datetime.utcfromtimestamp(enddate) > now:
                # Esta configuración está activa
                cur.execute("SELECT AIMED_TEMP FROM TEMPRANGE WHERE CONFIG_ID = ? AND TOP_GRAVITY >= ? AND BOTTOM_GRAVITY < ?", (configid, gravity, gravity,))
                aimedtemp = cur.fetchall()[0][0]
                cur.execute("SELECT IP, EMAIL, PASSWORD FROM TPLINK WHERE CONFIG_ID = ? AND TYPE = 'C'", (configid,))
                datosconge = cur.fetchall()[0]
                congelador = PyP100.P100(datosconge[0], datosconge[1], datosconge[2])
                cur.execute("SELECT IP, EMAIL, PASSWORD FROM TPLINK WHERE CONFIG_ID = ? AND TYPE = 'W'", (configid,))
                datoscale = cur.fetchall()[0]
                calefactor = PyP100.P100(datoscale[0], datoscale[1], datoscale[2])
                if temp > (aimedtemp + tolerance):
                    # Hay que enfriar el mosto!
                    congelador.turnOn()
                    calefactor.turnOff()
                    cur.execute("INSERT INTO ULOG (MOMENT, CONFIG_ID, EVENT) VALUES (CURRENT_TIMESTAMP, ?, ?)", (configid, "WARM TURNED OFF AND COLD TURNED ON",))
                elif temp < (aimedtemp - tolerance):
                    # Hay que calentar el mosto!
                    congelador.turnOff()
                    calefactor.turnOn()
                    cur.execute("INSERT INTO ULOG (MOMENT, CONFIG_ID, EVENT) VALUES (CURRENT_TIMESTAMP, ?, ?)", (configid, "WARM TURNED ON AND COLD TURNED OFF",))
                else:
                    cur.execute("INSERT INTO ULOG (MOMENT, CONFIG_ID, EVENT) VALUES (CURRENT_TIMESTAMP, ?, ?)", (configid, "WE ARE OK, NO TEMPERATURE CHANGE NEEDED",))
                conn.commit()
    # Cerramos la conexion
    conn.close()