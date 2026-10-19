from parser.parser import crc16_modbus

def test_positive_crc16_modbus():
    assert crc16_modbus(b"'ack':'ok';") == 0xF64E

def test_negative_crc16_modbus():
    assert crc16_modbus(b"'ack':'ok';") != 0xF64A
