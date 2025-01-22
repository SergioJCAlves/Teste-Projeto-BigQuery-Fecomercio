# src/scraper.py
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import time
import logging
import os
import shutil

class FecomercioScraper:
    def __init__(self):
        self.setup_logging()
        self.setup_download_dir()
        
    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
    def setup_download_dir(self):
        # Configurar diretório base do projeto
        self.project_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        
        # Configurar diretório de download
        self.download_dir = os.path.join(self.project_dir, 'downloads')
        if not os.path.exists(self.download_dir):
            os.makedirs(self.download_dir)
            
        self.logger.info(f"Download directory: {self.download_dir}")
            
    def setup_driver(self):
        chrome_options = Options()
        chrome_options.add_argument("--headless")  # Modo headless
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        # Comentamos esta linha que estava causando o erro
        # chrome_options.binary_location = "/usr/bin/google-chrome"
        
        # Configurar preferências de download
        prefs = {
            "download.default_directory": self.download_dir,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True,
            # Adicionadas configurações extras para garantir o download
            "profile.default_content_settings.popups": 0,
            "profile.default_content_setting_values.automatic_downloads": 1
        }
        
        chrome_options.add_experimental_option("prefs", prefs)
        
        # Adicionado try/except para melhor tratamento de erros na inicialização do driver
        try:
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=chrome_options)
            driver.command_executor._commands["send_command"] = ("POST", '/session/$sessionId/chromium/send_command')
            params = {'cmd': 'Page.setDownloadBehavior', 
                     'params': {'behavior': 'allow', 'downloadPath': self.download_dir}}
            driver.execute("send_command", params)
            return driver
        except Exception as e:
            self.logger.error(f"Erro ao configurar o Chrome driver: {str(e)}")
            raise
    
    def wait_for_download(self, timeout=60):  # Aumentado o timeout para 60 segundos
        """Espera até que o download seja concluído"""
        seconds = 0
        dl_wait = True
        while dl_wait and seconds < timeout:
            time.sleep(1)
            dl_wait = False
            files = os.listdir(self.download_dir)
            for fname in files:
                if fname.endswith('.crdownload') or fname.endswith('.tmp'):
                    dl_wait = True
            seconds += 1
            if seconds % 10 == 0:  # Log a cada 10 segundos
                self.logger.info(f"Aguardando download... {seconds} segundos decorridos")
        
        if seconds >= timeout:
            self.logger.error("Timeout ao aguardar download")
            return False
        return True
    
    def move_file_to_project_root(self, source_file, target_filename):
        """Move o arquivo baixado para a raiz do projeto"""
        target_path = os.path.join(self.project_dir, target_filename)
        
        # Se o arquivo de destino já existe, remove-o
        if os.path.exists(target_path):
            os.remove(target_path)
            
        # Move o arquivo
        shutil.move(source_file, target_path)
        self.logger.info(f"Arquivo movido para: {target_path}")
        
        return target_path
    
    def download_file(self, url, filename):
        driver = None
        try:
            self.logger.info(f"Iniciando download de {filename}")
            driver = self.setup_driver()
            
            # Adicionado timeout maior para carregar a página
            driver.set_page_load_timeout(30)
            driver.get(url)
            
            # Espera explícita pelo botão de download com retry
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    self.logger.info(f"Tentativa {attempt + 1} de encontrar o botão de download...")
                    download_button = WebDriverWait(driver, 20).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, "a.download"))
                    )
                    break
                except Exception as e:
                    if attempt == max_retries - 1:
                        raise
                    self.logger.warning(f"Erro ao encontrar botão, tentando novamente: {str(e)}")
                    time.sleep(5)
            
            # Limpa o diretório de download antes de baixar
            for file in os.listdir(self.download_dir):
                file_path = os.path.join(self.download_dir, file)
                try:
                    if os.path.isfile(file_path):
                        os.remove(file_path)
                except Exception as e:
                    self.logger.warning(f"Erro ao remover arquivo {file}: {str(e)}")
            
            # Clica no botão com retry
            max_click_retries = 3
            for attempt in range(max_click_retries):
                try:
                    download_button.click()
                    self.logger.info("Botão de download clicado")
                    break
                except Exception as e:
                    if attempt == max_click_retries - 1:
                        raise
                    self.logger.warning(f"Erro ao clicar no botão, tentando novamente: {str(e)}")
                    time.sleep(2)
            
            # Aguarda o download com timeout aumentado
            if not self.wait_for_download(timeout=60):
                raise TimeoutError("Download não completado no tempo esperado")
                
            # Encontra o arquivo baixado
            downloaded_files = os.listdir(self.download_dir)
            if not downloaded_files:
                raise FileNotFoundError("Nenhum arquivo encontrado após download")
                
            downloaded_file = os.path.join(self.download_dir, downloaded_files[0])
            
            # Aguarda um momento para garantir que o arquivo foi completamente escrito
            time.sleep(2)
            
            # Move o arquivo para a raiz do projeto
            final_path = self.move_file_to_project_root(downloaded_file, filename)
            
            # Verifica se o arquivo existe e tem tamanho maior que 0
            if not os.path.exists(final_path) or os.path.getsize(final_path) == 0:
                raise FileNotFoundError(f"Arquivo {filename} não encontrado ou vazio")
                
            self.logger.info(f"Download concluído: {final_path}")
            return final_path
            
        except Exception as e:
            self.logger.error(f"Erro ao baixar {filename}: {str(e)}")
            raise
            
        finally:
            if driver:
                try:
                    driver.quit()
                except Exception as e:
                    self.logger.warning(f"Erro ao fechar o driver: {str(e)}")
                
    def download_icc(self):
        return self.download_file(
            "https://www.fecomercio.com.br/pesquisas/indice/icc",
            "icc.xlsx"
        )
        
    def download_icf(self):
        return self.download_file(
            "https://www.fecomercio.com.br/pesquisas/indice/icf",
            "icf.xlsx"
        )