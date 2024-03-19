"""
Ingestión de datos - Reporte de clusteres
-----------------------------------------------------------------------------------------

Construya un dataframe de Pandas a partir del archivo 'clusters_report.txt', teniendo en
cuenta que los nombres de las columnas deben ser en minusculas, reemplazando los espacios
por guiones bajos; y que las palabras clave deben estar separadas por coma y con un solo 
espacio entre palabra y palabra.


"""
import pandas as pd
import warnings
import re

warnings.filterwarnings("ignore")


def ingest_data():
    anchos_de_columna = [9, 16, 16, 350]
    column_names = [
        "cluster",
        "cantidad_de_palabras_clave",
        "porcentaje_de_palabras_clave",
        "principales_palabras_clave",
    ]

    archivo_clusters = "clusters_report.txt"

    # Leer el archivo con el formato de ancho fijo
    cluster_df = pd.read_fwf(
        archivo_clusters,
        widths=anchos_de_columna,
        header=None,
        names=column_names,
    )

    # Eliminar las primeras filas innecesarias
    cluster_df = cluster_df.drop([0, 1, 2])

    # Resetear los índices del DataFrame
    cluster_df = cluster_df.reset_index(drop=True)

    # Convertir 'principales_palabras_clave' a tipo str
    cluster_df["principales_palabras_clave"] = cluster_df[
        "principales_palabras_clave"
    ].astype(str)

    # Reemplazar '%' con '', y ',' con '.' en 'porcentaje_de_palabras_clave' y convertirlo a tipo float
    cluster_df["porcentaje_de_palabras_clave"] = (
        cluster_df["porcentaje_de_palabras_clave"]
        .str.replace("%", "")
        .str.replace(",", ".")
        .astype(float)
    )

    # Llenar los valores vacíos en 'cluster', 'cantidad_de_palabras_clave' y 'porcentaje_de_palabras_clave'
    cluster_df["cluster"] = cluster_df["cluster"].fillna(method="pad")
    cluster_df["cantidad_de_palabras_clave"] = cluster_df[
        "cantidad_de_palabras_clave"
    ].fillna(method="pad")
    cluster_df["porcentaje_de_palabras_clave"] = cluster_df[
        "porcentaje_de_palabras_clave"
    ].fillna(method="pad")

    def join_words(words):
        return ' '.join(word.strip() for word in words if pd.notna(word))

    # Agrupar y juntar palabras clave
    clusters_report_df = (
        cluster_df.groupby(
            ["cluster", "cantidad_de_palabras_clave", "porcentaje_de_palabras_clave"],
            dropna=False,
        )["principales_palabras_clave"]
        .agg(join_words)
        .reset_index()
    )
    clusters_report_df["cluster"] = clusters_report_df["cluster"].astype(int)
    clusters_report_df = clusters_report_df.sort_values(by="cluster")
    clusters_report_df["principales_palabras_clave"] = (
        clusters_report_df["principales_palabras_clave"]
        .astype(str)
        .str.replace(",, ", ", ")
        .str.replace("   ", " ")
        .str.replace("  ", " ")
        .str.replace("hydrogen  production,  numeric", "hydrogen production, numeric")
        .str.replace(",  ", ", ")
        .str.replace(".", "")
        .str.lower()
    )
    clusters_report_df['principales_palabras_clave'] = clusters_report_df['principales_palabras_clave'].apply(lambda x: re.sub(r'([a-zA-Z]),([a-zA-Z])', r'\1, \2', x))
    clusters_report_df["cantidad_de_palabras_clave"] = clusters_report_df[
        "cantidad_de_palabras_clave"
    ].astype(int)


    return clusters_report_df


def main():
    cluster_df = ingest_data()


if __name__ == "__main__":
    main()

# df = ingest_data()
# print(df)