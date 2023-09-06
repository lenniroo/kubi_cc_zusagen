from didas.oracle import get_engine

def test_connection():
    engine = get_engine() 

    connection_working = True

    try:
        print("Versuche engine zu connecten")
        engine.connect()
        connection_working = True
        print("erfolg bei engine connect")
    except Exception as e:
        connection_working = False
        print("fehler bei Engine connect")
        open("err.txt", "w").write(str(e))
        print(str(e))

    assert connection_working