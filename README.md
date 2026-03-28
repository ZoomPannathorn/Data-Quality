# 📊 Data Validation Pipeline — Local Docker Setup

## 📁 Dataset: movie_ratings.csv

| Column        | Type  | Notes |
|--------------|------|------|
| Unnamed: 0   | int  | Original row index |
| movie        | str  | Movie title |
| year         | int  | Release year |
| imdb         | float| IMDB rating |
| metascore    | float| ⚠️ 850 nulls — triggers Split_Record branch |
| votes        | int  | Number of votes |

**Summary**
- Total rows: 1,800  
- Rows with null metascore: 850 (~47%)  
- Pipeline will always trigger Split_Record  

---

## 🏗️ Project Structure

```bash
project/
├── docker-compose.yaml
├── .env
├── dags/
│   ├── pipeline_dag.py
│   └── tasks/
│       ├── check_input.py
│       ├── issue_found.py
│       ├── split_record.py
│       └── convert_to_parquet.py
├── data/
│   ├── input/
│   ├── errors/
│   ├── clean/
│   └── parquet/
```

---

## 🚀 Quick Start

```bash
docker compose up -d
```

Open:
```
http://localhost:8080
```

Login:
```
admin / admin
```

---

## 🔄 Pipeline Flow

```
Check_Input → Issue_Found → Split_Record → Convert_to_parquet
```
<img width="1919" height="621" alt="image" src="https://github.com/user-attachments/assets/b15b07db-4603-4397-b80b-f88200d338a3" />
