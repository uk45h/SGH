USE database SGH;

SELECT IMIE, NAZWISKO, ULICA_ZAMIESZKANIA
FROM BAZA_KLIENTOW
WHERE REGEXP_LIKE(ULICA_ZAMIESZKANIA, '^ *P.*', 'i'); 
-- '^ *P' means: start of line, zero or more spaces, then the letter P. 
-- 'i' makes it case-insensitive.

-- Cwiczenie
-- Find all customers who live in a house number that is not a simple number (e.g., it contains a slash 37/36 or a letter 110A).
SELECT 
    IMIE, 
    NAZWISKO, 
    NUMER_DOMU
FROM BAZA_KLIENTOW
WHERE REGEXP_LIKE(NUMER_DOMU, 'XXXXXXXXX');
