import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
plt.close('all')

from sklearn.model_selection import train_test_split, cross_val_score, GridSearchCV
from sklearn.preprocessing import StandardScaler, MinMaxScaler
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.linear_model import LinearRegression
from sklearn.neighbors import KNeighborsRegressor
from sklearn.tree import DecisionTreeRegressor
from sklearn.neural_network import MLPRegressor
from sklearn.svm import SVR
from sklearn.metrics import r2_score, mean_absolute_percentage_error
import warnings
warnings.filterwarnings("ignore")

SEED = 60
np.random.seed(SEED)
XTrain = pd.read_csv("XTrain.csv",index_col=0)
XTest = pd.read_csv("XTest.csv",index_col=0)
ytrain = pd.read_csv("ytrain.csv",index_col=0).squeeze("columns")
ytest = pd.read_csv("ytest.csv",index_col=0).squeeze("columns")
# Train Test Split


print("//////////////////////////// 2. Algorithmen ///////////////////////////////////////////")

svr = SVR()
rf = RandomForestRegressor(random_state=SEED)
mlp = MLPRegressor()
gb = GradientBoostingRegressor()

print("/////////////////////////// 3. Training + Cross Validation ////////////////////////////")
# Dictionary to store CV scores
cv_scores_dict = {}
for clf in (svr, rf, mlp, gb):

    name = type(clf).__name__
    print("\n+++++++++++++++++++++++", name, "+++++++++++++++++++")

    clf.fit(XTrain, ytrain)

    print("Trainingsscore:", clf.score(XTrain, ytrain))

    cv_scores = cross_val_score(clf, XTrain, ytrain, cv=5)

    print("Crossval Durchschnitt:", cv_scores.mean())
    print("Alle CV Scores:", cv_scores)
    cv_scores_dict[name] = cv_scores.mean()
'''
print("/////////////////////////// 4. GridSearch ////////////////////////////////////////////")

param_svr = {
    "kernel": ["rbf"],
    "C": [0.1, 1, 10, 100],
    "gamma": ["scale", 0.01, 0.1, 1],
    "epsilon": [0.01, 0.1, 0.5, 1]
}
param_rf = {
    "n_estimators":[50,100,200,300],
    "max_depth":[5,10,20,30,None],
    "max_features":["sqrt","log2"]
}
param_mlp = {
    'hidden_layer_sizes': [(10,), (20), (20,20)],
    'alpha': [0.001, 0.01, 0.1],
    'tol': [0.0001],
    'learning_rate_init': [0.001, 0.01,0.1]
}
param_gb = {
        "n_estimators": [100, 200],
        "learning_rate": [0.01, 0.1, 0.2],
        "max_depth": [3, 5, 10]
}

for clf in (svr, rf, mlp, gb):

    name = type(clf).__name__

    if clf == svr:
        param_grid = param_svr

    elif clf == rf:
        param_grid = param_rf
    elif clf == mlp:
        param_grid = param_mlp
    elif clf == gb:
        param_grid = param_gb

    gs = GridSearchCV(clf, param_grid=param_grid, cv=5)

    gs.fit(XTrain, ytrain)

    print("GridSearch Score", name,
          cross_val_score(gs.best_estimator_, XTrain, ytrain).mean())

    print("Beste Parameter:", gs.best_params_)
    print()

'''
print("\n///////////////// 5. Scores on test data and model comparison/////////////////////")
# Define models with best parameters
models = {
    "SVR": SVR(kernel = 'rbf', C = 100, gamma = 0.1, epsilon = 0.1),
    "Random Forest": RandomForestRegressor(
        n_estimators=300,
        max_depth=30,
        max_features="sqrt",
        random_state=SEED),
    "MLP": MLPRegressor(
        hidden_layer_sizes = (20,20),
        alpha = 0.1,
        learning_rate_init = 0.001
    ),
    "gb": GradientBoostingRegressor(
        learning_rate = 0.2,
        max_depth = 5,
        n_estimators = 200,
    ),
}
# CV Scores
cv_scores = {
    "SVR": cv_scores_dict['SVR'],
    "Random Forest": cv_scores_dict['RandomForestRegressor'],
    "MLP": cv_scores_dict['MLPRegressor'],
    "gb": cv_scores_dict['GradientBoostingRegressor'],
}
# Prepare results list
results = []

# Loop through models
for i, model in models.items():
    # Train model
    model.fit(XTrain, ytrain)

    # Predict on test set
    y_pred = model.predict(XTest)

    # Calculate metrics
    r2 = r2_score(ytest, y_pred)
    mape = mean_absolute_percentage_error(ytest, y_pred)

    # Append all metrics
    results.append({
        "Model": i,
        "CV Mean": cv_scores[i],
        "Test R2": r2,
        "mape": mape,
    })
# Convert to DataFrame
results_df = pd.DataFrame(results)

# Display a clean table (CV folds as list optional)
pd.set_option('display.precision', 4)
print("\n================ Model Comparison =================\n")
print(results_df[["Model", "CV Mean", "Test R2", "mape"]])

