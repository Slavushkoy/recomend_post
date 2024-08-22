import os
from typing import List
from fastapi import FastAPI
from schema import PostGet, Response
from datetime import datetime
from catboost import CatBoostClassifier
from sqlalchemy import create_engine
import pandas as pd
import hashlib

SALT = b'some_random_salt'
GROUP_A_PERCENTAGE = 50
GROUP_B_PERCENTAGE = 50


def get_exp_group(user_id: int) -> str:
    hash_input = str(user_id).encode() + SALT
    hashed_value = hashlib.md5(hash_input).hexdigest()
    hash_int = int(hashed_value, 16)

    exp_group = 'control' if hash_int % 100 < GROUP_A_PERCENTAGE else 'test'

    return exp_group


def get_model_path(path: str, name: str) -> str:
    if os.environ.get("IS_LMS") == "1":  # проверяем где выполняется код в лмс, или локально. Немного магии
        MODEL_PATH = '/workdir/user_input/' + name
    else:
        MODEL_PATH = path
    return MODEL_PATH


def load_models(name: str):
    model_path = get_model_path("/my/super/path", name)
    model = CatBoostClassifier()
    model.load_model(model_path)
    return model


model_control = load_models('model_control')
model_test = load_models('model_test')


def batch_load_sql(query: str) -> pd.DataFrame:
    CHUNKSIZE = 200000
    engine = create_engine(
        "postgresql://robot-startml-ro:pheiph0hahj1Vaif@"
        "postgres.lab.karpov.courses:6432/startml"
    )
    conn = engine.connect().execution_options(stream_results=True)
    chunks = []
    for chunk_dataframe in pd.read_sql(query, conn, chunksize=CHUNKSIZE):
        chunks.append(chunk_dataframe)
    conn.close()
    return pd.concat(chunks, ignore_index=True)


def load_features(query: str) -> pd.DataFrame:
    return batch_load_sql(query)


post_features = load_features('SELECT * FROM slavushkoy_post_features_lesson_22')
user_features = load_features('SELECT * FROM slavushkoy_user_features_lesson_22')
post_features.columns = ['index', 'id', 'text', 'topic', 'len', 'maxtfidf', 'meantfidf']
feed = load_features(f'SELECT timestamp, user_id, post_id FROM public.feed_data  where target = 1')

def top_N(id, dt, limit):
    watched_post = feed[(feed['user_id'] == id) & (feed['timestamp'] < dt)]['post_id'].tolist()
    unwatched_post = post_features[~post_features['id'].isin(watched_post)].copy()
    df = unwatched_post[['topic', 'len', 'maxtfidf', 'meantfidf']].copy()
    df[['gender', 'age', 'country', 'city', 'exp_group', 'os', 'source', 'userViews', 'userMeans']] = \
        user_features[user_features['user_id'] == id][
            ['gender', 'age', 'country', 'city', 'exp_group', 'os', 'source', 'userViews', 'userMeans']].reset_index(
            drop=True).T[0].tolist()

    exp_group = get_exp_group(id)

    if exp_group == 'control':
        unwatched_post[['pred_0','pred_1']] = model_control.predict_proba(df)
    elif exp_group == 'test':
        unwatched_post[['pred_0','pred_1']] = model_test.predict_proba(df)
    else:
        raise ValueError('unknown group')

    recommendations = unwatched_post.sort_values('pred_1', ascending=False)[['id', 'text', 'topic']].head(limit).to_dict(
        orient='records')
    response = {
        'exp_group': exp_group,
        'recommendations': recommendations
    }

    return response


app = FastAPI()


@app.get("/post/recommendations/", response_model=Response)
def recommended_posts(
		id: int,
		time: datetime,
		limit: int = 10) -> Response:
    return top_N(id, time, limit)

