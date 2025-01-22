from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
from datetime import datetime
import logging
import os

class BigQueryLoader:
    def __init__(self):
        """Inicializa o loader configurando logging e credenciais"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger('loader')
        self.setup_credentials()
        self.dataset_id = f"{self.client.project}.sergio_maxpayne"

    def setup_credentials(self):
        """Configura as credenciais do BigQuery usando o arquivo de service account"""
        try:
            credentials_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'SA-maxpayne.json'))
            
            if not os.path.exists(credentials_path):
                raise FileNotFoundError(f"Arquivo de credenciais não encontrado em: {credentials_path}")
            
            credentials = service_account.Credentials.from_service_account_file(
                credentials_path,
                scopes=["https://www.googleapis.com/auth/cloud-platform"],
            )
            
            self.client = bigquery.Client(
                credentials=credentials,
                project=credentials.project_id,
            )
            
            self.logger.info("Credenciais do BigQuery configuradas com sucesso")
            
        except Exception as e:
            self.logger.error(f"Erro ao configurar credenciais: {str(e)}")
            raise

    def create_raw_tables(self):
        """Cria as tabelas raw com o schema correto"""
        try:
            tables_to_delete = ['icc_raw', 'icf_raw']
            for table_name in tables_to_delete:
                table_id = f"{self.dataset_id}.{table_name}"
                try:
                    self.client.delete_table(table_id)
                    self.logger.info(f"Tabela {table_name} excluída com sucesso")
                except Exception as e:
                    self.logger.info(f"Tabela {table_name} não existe ou não pôde ser excluída: {str(e)}")

            schema = [
                bigquery.SchemaField("ano_mes", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("indice", "FLOAT64"),
                bigquery.SchemaField("variacao_mes", "FLOAT64"),
                bigquery.SchemaField("variacao_ano", "FLOAT64"),
                bigquery.SchemaField("load_timestamp", "TIMESTAMP", mode="REQUIRED")
            ]

            for table_name in tables_to_delete:
                table_id = f"{self.dataset_id}.{table_name}"
                table = bigquery.Table(table_id, schema=schema)
                self.client.create_table(table, exists_ok=True)

            self.logger.info("Tabelas raw criadas/atualizadas com sucesso")

        except Exception as e:
            self.logger.error(f"Erro ao criar tabelas raw: {str(e)}")
            raise

    def process_excel_base(self, df, identifier):
        """Processa os dados do Excel garantindo consistência entre ICC e ICF"""
        try:
            self.logger.info(f"Processando arquivo Excel para {identifier}")
            
            # Log do DataFrame original
            self.logger.info(f"DataFrame original:\n{df.head()}")
            
            # Remove linhas e colunas completamente vazias
            df = df.dropna(how='all')
            df = df.dropna(axis=1, how='all')
            
            # Log após remoção de linhas e colunas vazias
            self.logger.info(f"DataFrame após remoção de vazios:\n{df.head()}")
            
            # Identifica a linha do índice principal baseado nas imagens fornecidas
            if identifier == 'ICC':
                main_row_idx = df[df.iloc[:, 0].astype(str).str.contains('Índice de Confiança do Consumidor', na=False)].index
            else:  # ICF
                main_row_idx = df[df.iloc[:, 0].astype(str).str.contains('ICF', na=False)].index
            
            # Verifica se a linha principal foi encontrada
            if main_row_idx.empty:
                self.logger.warning(f"Nenhuma linha principal encontrada para {identifier}.")
                return pd.DataFrame()  # Retorna um DataFrame vazio se não encontrar
            
            # Pega a linha do índice principal
            main_row = df.iloc[main_row_idx[0]]
            
            # Log da linha principal
            self.logger.info(f"Linha principal para {identifier}:\n{main_row}")
            
            # Pega o cabeçalho com as datas (primeira linha)
            dates = df.iloc[0, 1:-2]  # Exclui a primeira coluna e as duas últimas (variações)
            
            # Pega os valores do índice
            values = main_row.iloc[1:-2]  # Exclui a primeira coluna e as duas últimas (variações)
            
            # Pega as variações (últimas duas colunas com valores)
            var_mes = main_row.iloc[-2]
            var_ano = main_row.iloc[-1]
            
            # Verifica se var_mes e var_ano são strings antes de aplicar replace
            if isinstance(var_mes, str):
                var_mes = float(var_mes.replace('%', '').strip())
            elif isinstance(var_mes, (int, float)):
                var_mes = float(var_mes)  # Converte para float se já for numérico

            if isinstance(var_ano, str):
                var_ano = float(var_ano.replace('%', '').strip())
            elif isinstance(var_ano, (int, float)):
                var_ano = float(var_ano)  # Converte para float se já for numérico
            
            # Cria DataFrame limpo
            clean_df = pd.DataFrame({
                'ano_mes': dates,
                'indice': values,
                'variacao_mes': var_mes,
                'variacao_ano': var_ano,
                'load_timestamp': datetime.now()
            })
            
            # Remove linhas onde o índice é nulo
            clean_df = clean_df.dropna(subset=['indice'])
            
            # Padroniza formato ano_mes
            clean_df['ano_mes'] = clean_df['ano_mes'].apply(self.standardize_date)
            
            # Remove linhas com erro de conversão de data
            clean_df = clean_df.dropna()
            
            # Ordena por ano_mes
            clean_df = clean_df.sort_values('ano_mes')
            
            self.logger.info(f"Dados processados para {identifier}:")
            self.logger.info(f"Total de registros: {len(clean_df)}")
            self.logger.info(f"Amostra dos dados processados:\n{clean_df.head()}")
            
            return clean_df
            
        except Exception as e:
            self.logger.error(f"Erro ao processar arquivo Excel: {str(e)}")
            raise

    def standardize_date(self, date_str):
        """Converte datas no formato 'mmm-yy' para 'YYYY-MM'"""
        try:
            month_map = {
                'dez': '12', 'nov': '11', 'out': '10', 'set': '09',
                'ago': '08', 'jul': '07', 'jun': '06', 'mai': '05',
                'abr': '04', 'mar': '03', 'fev': '02', 'jan': '01'
            }
            
            if isinstance(date_str, str):
                parts = date_str.lower().split('/')
                if len(parts) == 2:
                    month, year = parts
                    if month in month_map:
                        return f"20{year}-{month_map[month]}"
            return None
            
        except Exception as e:
            self.logger.debug(f"Erro ao converter data '{date_str}': {str(e)}")
            return None

    def load_icc(self, file_path):
        """Carrega dados do ICC para o BigQuery"""
        try:
            self.logger.info(f"Iniciando carregamento do ICC do arquivo: {file_path}")
            df = pd.read_excel(file_path)
            clean_df = self.process_excel_base(df, 'ICC')
            
            if clean_df.empty:
                self.logger.warning("Nenhum dado para carregar no ICC.")
                return
            
            table_id = f"{self.dataset_id}.icc_raw"
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
            
            job = self.client.load_table_from_dataframe(
                clean_df, table_id, job_config=job_config
            )
            job.result()
            
            self.logger.info(f"Dados do ICC carregados com sucesso para {table_id}")
            
        except Exception as e:
            self.logger.error(f"Erro ao carregar ICC: {str(e)}")
            raise

    def load_icf(self, file_path):
        """Carrega dados do ICF para o BigQuery"""
        try:
            self.logger.info(f"Iniciando carregamento do ICF do arquivo: {file_path}")
            df = pd.read_excel(file_path)
            clean_df = self.process_excel_base(df, 'ICF')
            
            if clean_df.empty:
                self.logger.warning("Nenhum dado para carregar no ICF.")
                return
            
            table_id = f"{self.dataset_id}.icf_raw"
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
            
            job = self.client.load_table_from_dataframe(
                clean_df, table_id, job_config=job_config
            )
            job.result()
            
            self.logger.info(f"Dados do ICF carregados com sucesso para {table_id}")
            
        except Exception as e:
            self.logger.error(f"Erro ao carregar ICF: {str(e)}")
            raise

    def create_trusted_and_refined_tables(self):
        """Cria as tabelas trusted e refined"""
        try:
            sql_dir = os.path.join(os.path.dirname(__file__), 'sql')
            
            with open(os.path.join(sql_dir, 'create_trusted_tables.sql'), 'r') as f:
                trusted_sql = f.read()
            with open(os.path.join(sql_dir, 'create_refined_table.sql'), 'r') as f:
                refined_sql = f.read()
            
            self.client.query(trusted_sql).result()
            self.client.query(refined_sql).result()
            
            self.logger.info("Tabelas trusted e refined criadas com sucesso")
            
        except Exception as e:
            self.logger.error(f"Erro ao criar tabelas: {str(e)}")
            raise