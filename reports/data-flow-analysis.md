# Data Flow Analysis: API ETL Pipeline

## 1. Extract Phase

Para esta fase, el pipeline se conecta a la API pública de `jsonplaceholder.typicode.com`, usando dos endpoints: uno para traer la información de los usuarios (`/users`) y otro para sus publicaciones (`/posts`). En total se extraen 10 registros de usuarios y 100 de publicaciones, tal como se puede ver en el archivo `summary_YYYYMMDD.json`.

Las tareas `extract_users()` y `extract_posts()` corren al mismo tiempo porque no dependen una de la otra, así que no tiene sentido hacerlas esperar. Correrlas en paralelo es simplemente más eficiente: el pipeline termina más rápido aprovechando los recursos del sistema de forma simultánea.

## 2. Transform Phase

La función `transform_users()` se encarga de aplanar las partes anidadas del JSON original, como sacar `company_name`, `city`, `lat` y `lng` de sus diccionarios internos. También crea dos campos nuevos: `email_domain` (dividiendo el correo por el `@`) y `username_length`. Al final, filtra el DataFrame para quedarse solo con las columnas que realmente se necesitan.

Por su parte, `transform_posts()` añade métricas calculadas como `title_length`, `body_length` y `word_count`. Además clasifica cada post en una categoría (`short`, `medium` o `long`) según la longitud del texto, guardando ese resultado en una columna llamada `content_category`.

En ambos casos, pandas es el motor que hace el trabajo pesado. Los datos crudos, que llegan como listas de diccionarios, se convierten en DataFrames con `pd.DataFrame()`, lo que permite usar operaciones como `.apply()` con lambdas o `.str.split()` de forma mucho más limpia y rápida que recorrer los registros uno a uno con bucles.

## 3. Load Phase

Al final del pipeline, cada DataFrame transformado se guarda en dos formatos: CSV y JSON. Adicionalmente, la función `create_summary()` genera un reporte extra también en JSON. Todos los archivos se almacenan en `/opt/airflow/data` dentro del contenedor de Airflow, con la fecha de ejecución (`YYYYMMDD`) incluida en el nombre para mantener un historial ordenado.

Guardar en los dos formatos tiene una razón práctica: el CSV es cómodo para analistas que trabajen con Excel, herramientas de BI o bases de datos relacionales, mientras que el JSON es más útil para integraciones con otras aplicaciones o bases de datos NoSQL donde importa conservar la estructura original.

## 4. Data Flow

Con la TaskFlow API de Airflow (`@task`), los datos pasan de una tarea a otra de forma muy directa: cada función retorna sus resultados y la siguiente los recibe como parámetros, por ejemplo: `users_transformed = transform_users(users_raw)`.

Por debajo, Airflow usa XCom (Cross-Communication) para hacer esto posible. XCom es el mecanismo interno que permite a las tareas intercambiar datos entre sí. Con la TaskFlow API esto pasa de forma automática y transparente: cuando una tarea hace `return`, Airflow guarda ese valor en XCom, y cuando la siguiente tarea lo necesita, Airflow lo extrae y se lo entrega solo.

**Diagrama de dependencias de tareas:**

extract_users -----> transform_users -----> load_users 
                                                      \
                                                       -----> create_summary
                                                      /
extract_posts -----> transform_posts -----> load_posts 


