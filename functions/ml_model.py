#Importa bibliotecas
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, confusion_matrix, classification_report

#Import bases
data = pd.read_parquet("transient/ml-ready-df-gold.parquet")

# Seleção das features e do target
features = ['day_avgtemp_c', 'day_maxwind_kph', 'day_totalprecip_mm', 'day_avghumidity']
target = 'ocorrencia'

X = data[features]
y = data[target]

# Dividir em conjunto de treino e teste
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Criar o modelo de regressão logística
model = LogisticRegression()

# Treinar o modelo
model.fit(X_train, y_train)

# Fazer previsões no conjunto de teste
y_pred = model.predict(X_test)

# Previsão das probabilidades para todas as linhas do DataFrame original
data['probabilidade_ocorrencia'] = model.predict_proba(X)[:, 1]

#Salvando a base no ambiente transient
data.to_parquet("transient/ml_enhanced_gold.parquet")