from fastapi import APIRouter, HTTPException, File, UploadFile
import pandas as pd
from neo4j import GraphDatabase
from io import BytesIO

router = APIRouter()

# Assuming you have a function to create the Neo4j driver
def create_neo4j_driver():
    uri = "bolt://192.168.122.104:7687"
    username = "neo4j"
    password = "afmdpar"
    # uri = "bolt://localhost:7689"
    # username = "neo4j"
    # password = "password"
    return GraphDatabase.driver(uri, auth=(username, password))

# Assuming pandas DataFrame is passed as a file upload in FastAPI
expected_columns_kaspi = [
    'Дата и время операции', 'Валюта операции', 'Виды операции (категория документа)',
    'Сумма', 'Наименование/ФИО плательщика', 'ИИН/БИН плательщика',
    'Резидентство плательщика', 'Банк плательщика', 'Номер счета плательщика',
    'Наименование/ФИО получателя', 'ИИН/БИН получателя', 'Резидентство получателя',
    'Банк получателя', 'Номер счета получателя', 'Код назначения платежа', 'Назначение платежа'
]
expected_columns_halyk = [
    'Дата и время операции', 'Валюта операции', 'Виды операции (категория документа)',
    'Наименование СДП (при наличии)', 'Сумма в валюте ее проведения по кредиту',
    'Сумма в валюте ее проведения по дебету', 'Сумма в тенге',
    'Наименование/ФИО плательщика', 'ИИН/БИН плательщика', 'Резидентство плательщика',
    'Банк плательщика', 'Номер счета плательщика', 'Наименование/ФИО получателя',
    'ИИН/БИН получателя', 'Резидентство получателя', 'Банк получателя',
    'Номер счета получателя', 'Код назначения платежа', 'Назначение платежа'
]
expected_columns_home = [
    'Дата и время операции', 'Валюта операции', 'Виды операции (категория документа)',
    'Наименование СДП (при наличии)', 'Сумма в валюте ее проведения',
    'Сумма в тенге', 'Наименование/ФИО плательщика', 'ИИН/БИН плательщика',
    'Резидентство плательщика', 'Банк плательщика', 'Номер счета плательщика',
    'Наименование/ФИО получателя', 'ИИН/БИН получателя', 'Резидентство получателя',
    'Банк получателя', 'Номер счета получателя', 'Код назначения платежа',
    'Назначение платежа'
]
expected_columns_vtb = [
    'Дата и время операции', 'Валюта операции', 'Вид операции (КД)',
    'Наименование СДП (при наличии)', 'Сумма (вал.)', 'Сумма (тенге)',
    'Наименование/ФИО плательщика', 'ИИН/БИН плательщика', 'Резиденство плательщика',
    'Банк плательщика', 'Номер счета плательщика', 'Наименование/ФИО получателя',
    'ИИН/БИН получателя', 'Резиденство получателя', 'Банк получателя',
    'Номер счета получателя', 'Код назначение платежа (КНП)', 'Назначение платежа'
]


def determine_type(iin):
    iin_str = str(iin)
    if len(iin_str)>5:
        if iin_str[4] in {'0', '1', '2', '3'}:
            return "Person"
        elif iin_str[4] in {'4', '5', '6', '7', '8', '9'}:
            return "Company"
    else:
        return "rrrBank"


def determine_iin(entity_type):
    if entity_type == 'Person':
        return 'ИИН'
    elif entity_type == 'Company':
        return 'БИН'
    return None


def check_columns(df_columns, expected_columns):
    return all(column in df_columns for column in expected_columns)


@router.post("/insert_data_kaspi/")
async def insert_data_kaspi(file: UploadFile = File(...)):
    contents = await file.read()
    driver = create_neo4j_driver()

    df = pd.read_excel(BytesIO(contents), engine='openpyxl', skiprows=10)
    if not check_columns(df.columns, expected_columns_kaspi):
        raise HTTPException(status_code=400, detail="Неправильный формат файла для Каспи.")

    df.dropna(how='all', inplace=True)

    try:
        with driver.session() as session:
            for index, row in df.iterrows():
                try:
                    iin1 = determine_type(row['ИИН/БИН плательщика'])
                    iin2 = determine_type(row['ИИН/БИН получателя'])

                    property1 = determine_iin(iin1)
                    property2 = determine_iin(iin2)
                    if property1 is None or property2 is None:
                        raise ValueError(f"Invalid IIN/BIN types for row {index}")
                    query = f"""
                        MERGE (p1:{iin1} {{{property1}: '{str(row['ИИН/БИН плательщика'])}'}})
                        MERGE (p2:{iin2} {{{property2}: '{str(row['ИИН/БИН получателя'])}'}})
                        MERGE (p1)-[t:TransactionKaspi]-(p2)
                        ON CREATE SET t.`Дата и время операции` = '{str(row['Дата и время операции'])}',
                                      t.`Валюта операции` = '{str(row['Валюта операции'])}',
                                      t.`Виды операции (категория документа)` = '{str(row['Виды операции (категория документа)'])}',
                                      t.`Сумма` = '{(row['Сумма'])}',
                                      t.`Наименование/ФИО плательщика` = '{str(row['Наименование/ФИО плательщика'])}',
                                      t.`ИИН/БИН плательщика` = '{str(row['ИИН/БИН плательщика'])}',
                                      t.`Резидентство плательщика` = '{str(row['Резидентство плательщика'])}',
                                      t.`Банк плательщика` = '{str(row['Банк плательщика'])}',
                                      t.`Номер счета плательщика` = '{str(row['Номер счета плательщика'])}',
                                      t.`Наименование/ФИО получателя` = '{str(row['Наименование/ФИО получателя'])}',
                                      t.`ИИН/БИН получателя` = '{str(row['ИИН/БИН получателя'])}',
                                      t.`Резидентство получателя` = '{str(row['Резидентство получателя'])}',
                                      t.`Банк получателя` = '{str(row['Банк получателя'])}',
                                      t.`Номер счета получателя` = '{str(row['Номер счета получателя'])}',
                                      t.`Код назначения платежа` = '{str(row['Код назначения платежа'])}',
                                      t.`Назначение платежа` = '{str(row['Назначение платежа'])}'
                        """

                    session.run(query)
                except Exception as row_error:
                    print(f"Error processing row {index}: {row_error}")

            return {"status": "Данные импортированы успешно!"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        driver.close()


@router.post("/insert_data_halyk/")
async def insert_data_halyk(file: UploadFile = File(...)):
    driver = create_neo4j_driver()
    contents = await file.read()
    excel_data = BytesIO(contents)  # Wrap the bytes in BytesIO
    df = pd.read_excel(excel_data, engine='openpyxl', skiprows=9)
    if not check_columns(df.columns, expected_columns_halyk):
        raise HTTPException(status_code=400, detail="Неправильный формат файла для Халык.")
    try:
        with driver.session() as session:
            for index, row in df.iterrows():
                iin1 = determine_type(row['ИИН/БИН плательщика'])
                iin2 = determine_type(row['ИИН/БИН получателя'])

                property1 = determine_iin(iin1)
                property2 = determine_iin(iin2)

                query = f"""
                    MERGE (p1: {iin1} {{{property1}: '{row['ИИН/БИН плательщика']}'}})
                    MERGE (p2: {iin2} {{{property2}: '{row['ИИН/БИН получателя']}'}})
                    MERGE (p1)-[t:TransactionHalyk]-(p2)
                    ON CREATE SET t.`Дата и время операции` = '{row['Дата и время операции']}',
                                  t.`Валюта операции` = '{row['Валюта операции']}',
                                  t.`Виды операции (категория документа)` = '{row['Виды операции (категория документа)']}',
                                  t.`Наименование СДП (при наличии)` = '{row.get('Наименование СДП (при наличии)', '')}',
                                  t.`Сумма в валюте ее проведения по кредиту` = '{row.get('Сумма в валюте ее проведения по кредиту', '')}',
                                  t.`Сумма в валюте ее проведения по дебету` = '{row.get('Сумма в валюте ее проведения по дебету', '')}',
                                  t.`Сумма в тенге` = '{row.get('Сумма в тенге', '')}',
                                  t.`Наименование/ФИО плательщика` = '{row['Наименование/ФИО плательщика']}',
                                  t.`ИИН/БИН плательщика` = '{row['ИИН/БИН плательщика']}',
                                  t.`Резидентство плательщика` = '{row['Резидентство плательщика']}',
                                  t.`Банк плательщика` = '{row['Банк плательщика']}',
                                  t.`Номер счета плательщика` = '{row['Номер счета плательщика']}',
                                  t.`Наименование/ФИО получателя` = '{row['Наименование/ФИО получателя']}',
                                  t.`ИИН/БИН получателя` = '{row['ИИН/БИН получателя']}',
                                  t.`Резидентство получателя` = '{row['Резидентство получателя']}',
                                  t.`Банк получателя` = '{row['Банк получателя']}',
                                  t.`Номер счета получателя` = '{row['Номер счета получателя']}',
                                  t.`Код назначения платежа` = '{row['Код назначения платежа']}',
                                  t.`Назначение платежа` = '{row['Назначение платежа']}'
                """
                session.run(query)
            return {"status": "Данные импортированы успешно!"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        driver.close()

@router.post("/insert_data_vtb/")
async def insert_data_vtb(file: UploadFile = File(...)):
    driver = create_neo4j_driver()
    contents = await file.read()
    excel_data = BytesIO(contents)
    df = pd.read_excel(excel_data, engine='openpyxl')
    if not check_columns(df.columns, expected_columns_vtb):
        raise HTTPException(status_code=400, detail="Неправильный формат файла для ВТБ.")
    try:
        with driver.session() as session:
            for index, row in df.iterrows():
                iin1 = determine_type(row['ИИН/БИН плательщика'])
                iin2 = determine_type(row['ИИН/БИН получателя'])

                property1 = determine_iin(iin1)
                property2 = determine_iin(iin2)

                query = f"""
                MERGE (p1:{iin1} {{{property1}:'{row['ИИН/БИН плательщика']}'}})
                MERGE (p2:{iin2} {{{property2}: '{row['ИИН/БИН получателя']}'}})
                MERGE (p1)-[t:TransactionVtb]-(p2)
                ON CREATE SET t.Дата и время операции = '{row['Дата и время операции']}',
                              t.Валюта операции = '{row['Валюта операции']}',
                              t.Вид операции (КД) = '{row['Вид операции (КД)']}',
                              t.Наименование СДП (при наличии) = '{row.get('Наименование СДП (при наличии)', '')}',
                              t.Сумма (вал.) = '{row['Сумма (вал.)']}',
                              t.Сумма (тенге) = '{row['Сумма (тенге)']}',
                              t.Наименование/ФИО плательщика = '{row['Наименование/ФИО плательщика']}',
                              t.ИИН/БИН плательщика = '{row['ИИН/БИН плательщика']}',
                              t.Резиденство плательщика = '{row['Резиденство плательщика']}',
                              t.Банк плательщика = '{row['Банк плательщика']}',
                              t.Номер счета плательщика = '{row['Номер счета плательщика']}',
                              t.Наименование/ФИО получателя = '{row['Наименование/ФИО получателя']}',
                              t.ИИН/БИН получателя = '{row['ИИН/БИН получателя']}',
                              t.Резиденство получателя = '{row['Резиденство получателя']}',
                              t.Банк получателя = '{row['Банк получателя']}',
                              t.Номер счета получателя = '{row['Номер счета получателя']}',
                              t.Код назначение платежа (КНП) = '{row['Код назначение платежа (КНП)']}',
                              t.Назначение платежа = '{row['Назначение платежа']}'
            """

                session.run(query)

            return {"status": "Данные импортированы успешно!"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        driver.close()

@router.post("/insert_data_home/")
async def insert_data_home(file: UploadFile = File(...)):
    driver = create_neo4j_driver()
    contents = await file.read()
    excel_data = BytesIO(contents)
    df = pd.read_excel(excel_data, engine='openpyxl')
    if not check_columns(df.columns, expected_columns_home):
        raise HTTPException(status_code=400, detail="Неправильный формат файла для HomeBank.")
    try:
        with driver.session() as session:
            for index, row in df.iterrows():
                iin1 = determine_type(row['ИИН/БИН плательщика'])
                iin2 = determine_type(row['ИИН/БИН получателя'])
                property1 = determine_iin(iin1)
                property2 = determine_iin(iin2)

                query = f"""
                    MERGE (p1:{iin1} {{{property1}: '{row['ИИН/БИН плательщика']}'}})
                    MERGE (p2:{iin2} {{{property2}: '{row['ИИН/БИН получателя']}'}})
                    MERGE (p1)-[t:TransactionHome]-(p2)
                    ON CREATE SET t.`Дата и время операции` = '{row['Дата и время операции']}',
                                  t.`Валюта операции` = '{row['Валюта операции']}',
                                  t.`Виды операции (категория документа)` = '{row['Виды операции (категория документа)']}',
                                  t.`Наименование СДП (при наличии)` = '{row.get('Наименование СДП (при наличии)', '')}',
                                  t.`Сумма в валюте ее проведения` = '{row['Сумма в валюте ее проведения']}',
                                  t.`Сумма в тенге` = '{row['Сумма в тенге']}',
                                  t.`Наименование/ФИО плательщика` = '{row['Наименование/ФИО плательщика']}',
                                  t.`ИИН/БИН плательщика` = '{row['ИИН/БИН плательщика']}',
                                  t.`Резидентство плательщика` = '{row['Резидентство плательщика']}',
                                  t.`Банк плательщика` = '{row['Банк плательщика']}',
                                  t.`Номер счета плательщика` = '{row['Номер счета плательщика']}',
                                  t.`Наименование/ФИО получателя` = '{row['Наименование/ФИО получателя']}',
                                  t.`ИИН/БИН получателя` = '{row['ИИН/БИН получателя']}',
                                  t.`Резидентство получателя` = '{row['Резидентство получателя']}',
                                  t.`Банк получателя` = '{row['Банк получателя']}',
                                  t.`Номер счета получателя` = '{row['Номер счета получателя']}',
                                  t.`Код назначения платежа` = '{row['Код назначения платежа']}',
                                  t.`Назначение платежа` = '{row['Назначение платежа']}'
                """
                print(query)
                session.run(query)

            return {"status": "Данные импортированы успешно!"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        driver.close()
#
# @router.post("/insert_data_bereke/")
# async def insert_data_bereke(file: UploadFile = File(...)):
#     driver = create_neo4j_driver()
#     contents = await file.read()
#     excel_data = BytesIO(contents)
#     df = pd.read_excel(excel_data, engine='openpyxl', skiprows=10, header=[0, 1])
#     try:
#         with driver.session() as session:
#             for index, row in df.iterrows():
#                 iin = None
#                 if isinstance(row['Корреспондент Счет, наименование БИН/ИИН'], str):
#                     iin = row['Корреспондент Счет, наименование БИН/ИИН'][21:33]
#
#                 if isinstance(row['Назначение платежа'], str):
#                     iin_match = re.search(r'ИИН\s(\d{12})', row['Назначение платежа'])
#                 if iin_match:
#                     print(iin, iin_match.group(1))
#
#                 query = """
#                 MERGE (p1:Person {iin: $iin})
#                 MERGE (p1)-[t:TransactionBereke]-(p2:Bank {bic_name: '55554'})
#                 ON CREATE SET t.`№ документа` = $doc_number,
#                               t.`Дата транзакции` = $transaction_date,
#                               t.`Время транзакции` = $transaction_time,
#                               t.`Корреспондент БИК наименование Банка` = $corr_bic_name,
#                               t.`Корреспондент Счет, наименование БИН/ИИН` = $corr_account_bin,
#                               t.`Обороты в валюте счета Дебет` = $debit_turnover,
#                               t.`Обороты в валюте счета Кредит` = $credit_turnover,
#                               t.`Назначение платежа` = $payment_description,
#                               t.`Коды ЕКНП` = $eknp_codes
#                 """
#                 session.run(query,
#                             iin=iin,
#                             doc_number=row['№ документа'],
#                             transaction_date=row['Дата транзакции'],
#                             transaction_time=row['Время транзакции'],
#                             corr_bic_name=row['Корреспондент БИК наименование Банка'],
#                             corr_account_bin=row['Корреспондент Счет, наименование БИН/ИИН'],
#                             debit_turnover=row['Обороты в валюте счета Дебет'],
#                             credit_turnover=row['Обороты в валюте счета Кредит'],
#                             payment_description=row['Назначение платежа'],
#                             eknp_codes=row['Коды ЕКНП'])
#
#             return {"status": "Data inserted for Bereke"}
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))
#     finally:
#         driver.close()
#
# @router.post("/insert_data_bck/")
# async def insert_data_bck(file: UploadFile = File(...)):
#     driver = create_neo4j_driver()
#     contents = await file.read()
#     df = pd.read_excel(contents, engine='openpyxl', skiprows=10)
#     df.dropna(how='all', inplace=True)
#
#     try:
#         with driver.session() as session:
#             for index, row in df.iterrows():
#                 if isinstance(row['Корресп. Банк / Банк корресп.'], str):
#                     iinr = row['Корресп. Банк / Банк корресп.'][10:22]
#                 if pd.notna(iinr) and pd.notna(row['ИИН/БИН контрагента']):
#                     iin1 = determine_type(iinr)
#                     iin2 = determine_type(row['ИИН/БИН контрагента'])
#                 query = f"""
#                        Merge (p1:{iin1} {{iin: $iinr}})
#                        Merge (p2:{iin2} {{iin: $counterparty_bin}})
#                        MERGE (p1)-[t:TransactionCenterCredit]->(p2)
#                        ON CREATE SET t.`Күні / Дата` = $date,
#                                      t.`Счет` = $account,
#                                      t.`Дата открытия` = $open_date,
#                                      t.`Дата Закрытия` = $close_date,
#                                      t.`Құжат № /№ Документа` = $document_number,
#                                      t.`Валютасы /Валюта` = $currency,
#                                      t.`Корресп. Банк / Банк корресп.` = $correspondent_bank,
#                                      t.`Корресп. Есепшоты / Счет-корреспондент` = $correspondent_account,
#                                      t.`АЖК /БеК / КОд /Кбе` = $ajk_bek_code,
#                                      t.`ИИН/БИН контрагента` = $counterparty_bin,
#                                      t.`Контрагент. атауы / Наименование контрагента` = $counterparty_name,
#                                      t.`Дебет айналымы / Дебетовый оборот` = $debit_turnover,
#                                      t.`Кредит айналымы / Кредитовый оборот` = $credit_turnover,
#                                      t.`Төлем мақсаты / Назначение платежа` = $payment_purpose
#                        """
#
#                 session.run(query,
#                             iinr=iinr,
#                             date=row['Күні / Дата'],
#                             account=row['Счет'],
#                             open_date=row['Дата открытия'],
#                             close_date=row['Дата Закрытия'],
#                             document_number=row['Құжат № /№ Документа'],
#                             currency=row['Валютасы /Валюта'],
#                             correspondent_bank=row['Корресп. Банк / Банк корресп.'],
#                             correspondent_account=row['Корресп. Есепшоты / Счет-корреспондент'],
#                             ajk_bek_code=row['АЖК /БеК / КОд /Кбе'],
#                             counterparty_bin=row['ИИН/БИН контрагента'],
#                             counterparty_name=row['Контрагент. атауы / Наименование контрагента'],
#                             debit_turnover=row['Дебет айналымы / Дебетовый оборот'],
#                             credit_turnover=row['Кредит айналымы / Кредитовый оборот'],
#                             payment_purpose=row['Төлем мақсаты / Назначение платежа'])
#
#             return {"status": "Data inserted for BCK"}
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))
#     finally:
#         driver.close()

# @app.post("/insert_data_euroasia/")
# async def insert_data_euroasia(df: pd.DataFrame):
#     driver = create_neo4j_driver()
#     try:
#         with driver.session() as session:
#             for index, row in df.iterrows():
#                 iin1 = determine_type(row['ИИН/БИН Бенефициара/отправителя'])
#                 query = f"""
#                 MERGE (p1:{iin1} {{iin: $beneficiary_bin}})
#                 MERGE (p2:neznau {{iin: '4545'}})
#                 MERGE (p1)-[t:TransactionEuroAsia]->(p2)
#                 ON CREATE SET t.`Дата проводки` = $date,
#                               t.`Вид операции` = $operation_type,
#                               t.`Номер документа клиента` = $doc_number,
#                               t.`Наименование Бенефициара/Отправителя` = $beneficiary_name,
#                               t.`ИИН/БИН Бенефициара/отправителя` = $beneficiary_bin,
#                               t.`ИИК бенефициара/Отправителя денег` = $beneficiary_ikk,
#                               t.`Наименование банка Бенефициара/Отправителя` = $beneficiary_bank_name,
#                               t.`БИК банка Бенефициара/Отправителя` = $bank_bik,
#                               t.`Назначение платежа` = $payment_purpose,
#                               t.`Дебет` = $debit,
#                               t.`Кредит` = $credit,
#                               t.`Остаток` = $balance
#                 """
#                 session.run(query,
#                             date=row['Дата проводки'],
#                             operation_type=row['Вид операции'],
#                             doc_number=row['Номер документа клиента'],
#                             beneficiary_name=row['Наименование Бенефициара/Отправителя '],
#                             beneficiary_bin=row['ИИН/БИН Бенефициара/отправителя'],
#                             beneficiary_ikk=row['ИИК бенефициара/Отправителя денег '],
#                             beneficiary_bank_name=row['Наименование банка Бенефициара/Отправителя'],
#                             bank_bik=row['БИК банка Бенефициара/Отправителя'],
#                             payment_purpose=row['Назначение платежа'],
#                             debit=row['\nДебет'],
#                             credit=row['\nКредит'],
#                             balance=row['Остаток'])
#
#             return {"status": "Data inserted for Euroasia"}
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))
#     finally:
#         driver.close()
#
# @app.post("/insert_data_zhusan/")
# async def insert_data_zhusan(df: pd.DataFrame):
#     driver = create_neo4j_driver()
#     try:
#         with driver.session() as session:
#             for index, row in df.iterrows():
#                 iin1 = determine_type(row['БИН/ИИН бенефициара/контрагента'])
#
#                 query = f"""
#                 MERGE (p1:{iin1} {{iin: $counterparty_bin_iin}})
#                 MERGE (p2:neznau {{iin: '45467'}})
#                 MERGE (p1)-[t:TransactionZhusan]-(p2)
#                 ON CREATE SET t.`Дата` = $date,
#                               t.`Дата документа` = $document_date,
#                               t.`№ Документа` = $doc_number,
#                               t.`Вид операции` = $operation_type,
#                               t.`БИК/SWIFT` = $bik_swift,
#                               t.`Наименование банка плательщика/получателя` = $bank_name,
#                               t.`Счет-корреспондент` = $correspondent_account,
#                               t.`Наименование контрагента` = $counterparty_name,
#                               t.`БИН/ИИН бенефициара/контрагента` = $counterparty_bin_iin,
#                               t.`Дебетовый оборот` = $debit_turnover,
#                               t.`Кредитовый оборот` = $credit_turnover,
#                               t.`Сумма эквивалента в тенге` = $amount_tenge,
#                               t.`Назначение платежа` = $payment_purpose,
#                               t.`КНП` = $knp
#                 """
#                 print(row)
#                 session.run(query,
#                             date=row.get('Дата', None),
#                             document_date=row.get('Дата документа', None),
#                             doc_number=row.get('№ Документа', None),
#                             operation_type=row.get('Вид операции', None),
#                             bik_swift=row.get('БИК/SWIFT', None),
#                             bank_name=row.get('Наименование банка плательщика/получателя', None),
#                             correspondent_account=row.get('Счет-корреспондент', None),
#                             counterparty_name=row.get('Наименование контрагента', None),
#                             counterparty_bin_iin=row.get('БИН/ИИН бенефициара/контрагента'),
#                             debit_turnover=row.get('Дебетовый оборот', None),
#                             credit_turnover=row.get('Кредитовый оборот', None),
#                             amount_tenge=row.get('Сумма эквивалента в тенге', None),
#                             payment_purpose=row.get('Назначение платежа', None),
#                             knp=row.get('КНП', None))
#             return {"status": "Data inserted for Zhusan"}
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))
#     finally:
#         driver.close()
