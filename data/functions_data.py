from datetime import date, timedelta

def round_datetime_to_nearest_hour(dt):
    """
    Arredonda um objeto datetime para a hora mais próxima.
    
    Parâmetros:
    - dt (datetime): Objeto datetime a ser arredondado.
    
    Retorno:
    - datetime: Objeto datetime arredondado para a hora mais próxima.
    """
    # Verifica se os minutos são 30 ou mais para arredondar para a próxima hora
    if dt.minute >= 30:
        # Arredonda para a próxima hora
        dt = dt.replace(second=0, microsecond=0) + timedelta(hours=1, minutes=-dt.minute)
    else:
        # Mantém a hora atual, ajustando segundos e microssegundos
        dt = dt.replace(minute=0, second=0, microsecond=0)
    
    return dt