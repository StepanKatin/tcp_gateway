import asyncio
#contract_example = '"imei":"129464373";"datetime":"12-54-46";"speed_can":"77";"speed_engine":"1500";"dut_lvl":"1024";"crc16":"128127"'

def crc16_modbus(data: bytes) -> int:
    """
    Рассчитывает контрольную сумму CRC-16 Modbus для переданного массива байт.
    Возвращает целое число (16-битное).
    """
    # Инициализация регистра CRC значением 0xFFFF
    crc = 0xFFFF
    
    # Константа полинома инвертированного CRC-16 (0xA001)
    POLY = 0xA001
    
    for byte in data:
        # XOR текущего байта со старшей или младшей частью CRC (для Modbus — с младшей)
        crc ^= byte
        
        # Цикл по всем 8 битам байта
        for _ in range(8):
            if crc & 1:
                # Если младший бит равен 1, сдвигаем вправо и делаем XOR с полиномом
                crc = (crc >> 1) ^ POLY
            else:
                # Если младший бит равен 0, просто сдвигаем вправо
                crc >>= 1
                
    return crc

def parse_request(data: bytes) -> dict:
    raw = data.decode("utf-8", errors="ignore").strip()
    
    # 1. Защита от пустых пакетов
    if not raw or ";crc16:" not in raw:
        raise ValueError("Invalid packet format: missing crc16 field")
    if "imei:" not in raw:
        raise ValueError("Invalid packet format: missing imei field")

    # 2. РАЗДЕЛЯЕМ данные и CRC16
    # data_part будет: "imei:129464373;datetime:12-54-46;speed_can:77;speed_engine:1500;dut_lvl:1024"
    # crc_part будет: "128127"
    data_part, crc_part = raw.split(";crc16:", 1)
    
    # 3. Парсим поля
    result = {}
    splitted = raw.split(";")
    if len(splitted) != 6:
        raise ValueError("Data from source is not correct")
        
    for r in splitted:
        key, value = r.split(":", 1)
        result[key] = value
        
    try:
        crc_as_int = int(crc_part)
    except ValueError:
        raise ValueError(f"CRC16 must be an integer: {crc_part}")

    # 4. ЧЕСТНАЯ ПРОВЕРКА: считаем CRC от полезных данных (переводим их обратно в bytes)
    if crc16_modbus(data=data_part.encode("utf-8")) != crc_as_int:
        raise ValueError("CRC16 checksum mismatch")

    return result
