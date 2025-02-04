from mrjob.job import MRJob
from mrjob.step import MRStep
import re


class WordCountRomeo(MRJob):

    # Define el argumento adicional --word
    def configure_args(self):
        super(WordCountRomeo, self).configure_args()
        self.add_passthru_arg('--word', type=str, help='Palabra a contar')

    def steps(self):
        return [
            MRStep(mapper=self.mapper_gert_palabras,
                   reducer=self.reducer_cuenta_palabras)
        ]

    def mapper_gert_palabras(self, _, line):
        palabra_a_contar = self.options.word.lower()
        limpiando_linea = re.sub(r"[¡¿\"“”']|[\d]|[^\w\s]", '', line.lower())
        palabras = limpiando_linea.split()
        
        for palabra in palabras:
            if palabra == palabra_a_contar:
                yield palabra, 1

    def reducer_cuenta_palabras(self, key, values):
        yield key, sum(values)
            

if __name__ == '__main__':
    WordCountRomeo.run()
