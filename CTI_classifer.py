import numpy
from sklearn.model_selection import train_test_split
from sklearn import metrics
from sklearn.ensemble import RandomForestClassifier
import pandas as ps
import statistics
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer
import nltk


number_of_classifiers = 11
nltk.download('punkt')
lem = WordNetLemmatizer()

def get_random_forest_classifiers(number, X, y):
    classifiers = []
    for i in range(number):
        x_train, x_test, y_train, y_test = train_test_split(X, y, test_size=0.1, random_state=i, stratify=y)
        estimator = RandomForestClassifier(n_estimators=300, max_features=.15, criterion='entropy', min_samples_split=4)
        estimator.fit(x_train, y_train)
        classifiers.append(estimator)
    return classifiers

def construct_classifier():
    # For training the classifier
    master_Table = ps.read_csv(r'dataset\training-set.csv', delimiter=',')
    X, y = master_Table.iloc[:, 1:-1], master_Table.iloc[:, -1]
    classifiers = get_random_forest_classifiers(number_of_classifiers, X, y)
    columns = master_Table.head()

    column_names = []
    for col in columns:
        column_names.append(col)
    column_names = column_names[1:-3]

    return classifiers, column_names


def vectorize(tweet, vocab):
    vector = []

    tweet = tweet.lower()
    bag = word_tokenize(tweet)

    for i in vocab:
        count = 0
        for j in bag:
            if i == j:
                count += 1
        vector.append(count)

    return vector


if __name__ == '__main__':
    final_estimate = []

    classifiers, col_names = construct_classifier()

    # For evaluating the classifier
    test_table = ps.read_csv(r'dataset\test-set-random.csv', delimiter=',')

    x2, y2 = test_table.iloc[:, 1:-1], test_table.iloc[:, -1]

    estimates = []
    for i in range(number_of_classifiers):
        y_estimate = classifiers[i].predict(x2)
        estimates.append(y_estimate)

    aggregated_results = []
    n = 0

    # do the majority voting here
    while n < 143:
        Y=0
        N=0
        R=0
        for i in estimates:
            vote = i[n]
            aggregated_results.append(vote)
        final_estimate.append(statistics.mode(aggregated_results))
        aggregated_results.clear()
        n+=1
    accuracy = metrics.accuracy_score(y_true=y2, y_pred=final_estimate)
    print(accuracy)
