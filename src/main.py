from scraper import FecomercioScraper
from loader import BigQueryLoader
import logging

def main():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    try:
        scraper = FecomercioScraper()
        icc_path = scraper.download_icc()
        icf_path = scraper.download_icf()

        loader = BigQueryLoader()
        # Adicionando a criação das tabelas raw antes do carregamento
        loader.create_raw_tables()
        
        loader.load_icc(icc_path)
        loader.load_icf(icf_path)
        loader.create_trusted_and_refined_tables()

        logger.info("Pipeline concluído com sucesso")
    except Exception as e:
        logger.error(f"Erro no pipeline: {str(e)}")

if __name__ == "__main__":
    main()
