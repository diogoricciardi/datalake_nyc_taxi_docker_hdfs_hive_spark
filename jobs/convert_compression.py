'''
Motivo: o libhadoop usado pelo cluster Spark/Hadoop deste projeto (imagens
bde2020) não foi compilado com suporte a zstd. Isso é uma limitação comum
do Hadoop em geral, não específica dessa imagem (ver SPARK-34651). O Spark
consegue ler o SCHEMA de um parquet em zstd normalmente, mas falha ao
tentar descomprimir o conteúdo de fato (erro: "native zStandard library
not available").
'''

import pathlib
import pyarrow.parquet as pq

raw_dir = pathlib.Path(__file__).parent.parent / "data" / "raw" / "2025"

# codec de destino, compatível nativamente com o cluster Spark/Hadoop
codec = "snappy"

def main():
    # sorted transforma o generator (objeto que produz resultados 1 de cada vez) em uma lista ordenada
    files = sorted(raw_dir.glob("*.parquet"))
    
    if not files:
        print(f"No parquet files found in {raw_dir}.")
        return
    
    print(f"{len(files)} parquet files found in {raw_dir}.")
    
    for file in files:
        table = pq.read_table(file)
        current_codec = pq.ParquetFile(file).metadata.row_group(0).column(0).compression

        if current_codec.lower() == codec:
            print(f"Skipping file {file.name} because it is already in {codec}.")
            continue
        
        print(f"{file.name}: Converting codec from {current_codec} to {codec}.")
        
        pq.write_table(table, file, compression=codec)
        
    print("Conversion completed.")
    
if __name__ == "__main__":
    main()    
