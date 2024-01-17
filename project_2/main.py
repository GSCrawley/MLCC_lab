from flask import Flask, request, jsonify, render_template, redirect, url_for
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression

app = Flask(__name__)

# Load the CSV file and prepare the dataset
df = pd.read_csv('Salary_dataset.csv')
X = df[['YearsExperience']]
y = df['Salary']
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=0
)

# Create and train the Linear Regression model
model = LinearRegression()
model.fit(X_train, y_train)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/predict', methods=['POST'])
def predict_salary():
    content = request.json
    years_exp = float(content['yearsExperience'])
    new_data = pd.DataFrame([[years_exp]], columns=['YearsExperience'])
    salary_pred = model.predict(new_data)[0]

    # Convert salary_pred to float for url_for
    return redirect(url_for('result', 
        years_experience=years_exp, 
        predicted_salary=float(salary_pred)
    ))

@app.route('/result')
def result():
    # Get values passed as query parameters
    years_exp = request.args.get('years_experience', type=float)
    predicted_salary = request.args.get('predicted_salary', type=float)
    return render_template(
        'result.html',
        years_experience=years_exp,
        predicted_salary=predicted_salary
    )

if __name__ == '__main__':
    app.run(debug=True)