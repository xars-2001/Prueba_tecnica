from flask import Flask, request, jsonify

app = Flask(__name__)

class NumberSet:
    def __init__(self):
        # Inicializa el conjunto con los números del 1 al 100
        self.numbers = set(range(1, 101))
        self.extracted_number = None

    def extract(self, number):
        # Valida que el número sea un entero entre 1 y 100
        if not isinstance(number, int) or number < 1 or number > 100:
            raise ValueError("El número de entrada debe ser un entero entre 1 y 100")
        
        # Valida que el número esté en el conjunto
        if number not in self.numbers:
            raise ValueError(f"El número {number} no está entre los primeros 100 números naturales")
        
        # Remueve el número del conjunto y lo guarda como extraído
        self.numbers.remove(number)
        self.extracted_number = number

    def calculate_missing_number(self):
        # Verifica que exactamente un número haya sido extraído
        if len(self.numbers) == 99:
            # Calcula el número faltante
            missing_number = (set(range(1, 101)) - self.numbers).pop()
            return missing_number
        else:
            raise ValueError("Debe extraerse exactamente un número para calcular el número faltante")

# Inicializa el conjunto de números
set_of_numbers = NumberSet()

#Extrae el número, valida que sea correcto.
@app.route('/extract', methods=['POST'])
def extract_number():
    data = request.get_json()
    if 'number' not in data:
        return jsonify({"error": "Falta el parámetro 'number'"}), 400
    
    try:
        number = int(data['number'])
        set_of_numbers.extract(number)
        return jsonify({"extracted_number": set_of_numbers.extracted_number}), 200
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

#Calcula el número faltante
@app.route('/missing', methods=['GET'])
def missing_number():
    try:
        missing_number = set_of_numbers.calculate_missing_number()
        return jsonify({"missing_number": missing_number}), 200
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

if __name__ == '__main__':
    app.run(debug=True)
