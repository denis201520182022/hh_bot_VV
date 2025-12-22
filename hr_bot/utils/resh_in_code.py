def is_candidate_profile_complete(candidate) -> bool:
    """
    Проверяет, заполнены ли все обязательные поля анкеты в БД.
    """
    if not candidate:
        return False
        
    # Проверяем наличие значений (не None и не пустая строка)
    has_phone = bool(candidate.phone_number)
    has_city = bool(candidate.city)
    
    has_citizenship = bool(candidate.citizenship)
    # Возраст проверяем именно на None, так как 0 — это теоретически валидное число (хоть и не для работы)
    has_age = candidate.age is not None 

    return all([has_phone, has_citizenship, has_age])

def check_candidate_eligibility(candidate) -> bool:
    """
    Проверяет кандидата по жестким критериям (Возраст + Гражданство).
    Возвращает True, если кандидат подходит.
    """
    # 1. Проверка возраста (18-58 включительно)
    if not (18 <= candidate.age <= 58):
        return False
    
    # 2. Проверка гражданства
    # Список стран, граждане которых подходят без дополнительных условий
    allowed_countries = ['рф', 'еаэс', "внж рф", "рвп рф"]
    cand_citizen = (candidate.citizenship or "").lower().strip()
    
    # Если страна из списка найдена в строке гражданства - ОК
    if any(country in cand_citizen for country in allowed_countries):
        return True
        
    # Если другая страна, ищем упоминание ВНЖ или РВП
    if 'внж' in cand_citizen or 'рвп' in cand_citizen or 'вид на жительство' in cand_citizen:
        return True
        
    return False