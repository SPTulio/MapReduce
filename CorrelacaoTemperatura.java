import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * Correlacao de Pearson entre a coluna 9 (temp) e as demais colunas
 * do arquivo forestfireinput.csv usando MapReduce.
 *
 * Colunas: X, Y, month, day, FFMC, DMC, DC, ISI, temp, RH, wind, rain, area
 *
 * A correlacao nao pode ser calculada separadamente em cada slave, porque
 * cada um tem so um bloco do arquivo (a correlacao de um pedaco e diferente
 * da correlacao do arquivo inteiro). Entao:
 *  - Map: cada slave le TODAS as linhas do seu bloco e vai somando
 *    n, soma(x), soma(y), soma(x^2), soma(y^2) e soma(x*y).
 *    So no final do bloco (cleanup) ele envia essas somas para o Reduce.
 *  - Reduce: junta as somas de todos os slaves e so entao calcula a
 *    correlacao, usando os dados completos.
 */
public class CorrelacaoTemperatura {

    static final String[] COLUNAS = {"X", "Y", "month", "day", "FFMC", "DMC", "DC",
                                     "ISI", "temp", "RH", "wind", "rain", "area"};
    static final int TEMP = 8; // coluna 9 (o indice comeca em 0)

    static final String MESES = "jan feb mar apr may jun jul aug sep oct nov dec";
    static final String DIAS = "mon tue wed thu fri sat sun";

    // Contadores que aparecem no final da execucao do job
    enum Linhas { LIDAS, VALIDAS, INVALIDAS }

    // ---------------------------------------------------------------- MAP
    public static class MapCorrelacao extends Mapper<LongWritable, Text, Text, Text> {

        // somas[coluna] = {n, sx, sy, sxx, syy, sxy}   (x = temp, y = coluna)
        private double[][] somas = new double[COLUNAS.length][6];

        @Override
        public void map(LongWritable chave, Text valor, Context context) {
            context.getCounter(Linhas.LIDAS).increment(1);

            double[] v = converterLinha(valor.toString());
            if (v == null) {
                // linha incompleta ou com valor invalido: descarta a linha inteira
                context.getCounter(Linhas.INVALIDAS).increment(1);
                return;
            }
            context.getCounter(Linhas.VALIDAS).increment(1);

            double x = v[TEMP];
            for (int i = 0; i < COLUNAS.length; i++) {
                if (i == TEMP) continue;
                double y = v[i];
                somas[i][0] += 1;
                somas[i][1] += x;
                somas[i][2] += y;
                somas[i][3] += x * x;
                somas[i][4] += y * y;
                somas[i][5] += x * y;
            }
        }

        // Executado uma vez, depois que o slave leu o bloco inteiro
        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            for (int i = 0; i < COLUNAS.length; i++) {
                if (i == TEMP || somas[i][0] == 0) continue;
                String s = somas[i][0] + ";" + somas[i][1] + ";" + somas[i][2] + ";"
                         + somas[i][3] + ";" + somas[i][4] + ";" + somas[i][5];
                context.write(new Text(nomeColuna(i)), new Text(s));
            }
        }

        // Transforma a linha em 13 numeros. Retorna null se a linha nao estiver integra.
        static double[] converterLinha(String linha) {
            String[] campos = linha.trim().split(",", -1);
            if (campos.length != COLUNAS.length) return null;

            double[] v = new double[COLUNAS.length];
            try {
                for (int i = 0; i < campos.length; i++) {
                    String c = campos[i].trim().toLowerCase();
                    if (i == 2) {
                        v[i] = posicao(MESES, c);      // jan = 1 ... dec = 12
                    } else if (i == 3) {
                        v[i] = posicao(DIAS, c);       // mon = 1 ... sun = 7
                    } else {
                        v[i] = Double.parseDouble(c);
                    }
                    if (v[i] < 0 && i != TEMP) return null;   // mes/dia invalido ou valor negativo
                    if (Double.isNaN(v[i]) || Double.isInfinite(v[i])) return null;
                }
            } catch (NumberFormatException e) {
                return null; // texto onde deveria ter numero (ex.: cabecalho)
            }

            // valores impossiveis: X e Y vao de 1 a 9, FFMC ate 101, umidade (RH) ate 100%
            if (v[0] < 1 || v[0] > 9 || v[1] < 1 || v[1] > 9 || v[4] > 101 || v[9] > 100) {
                return null;
            }
            return v;
        }

        static int posicao(String lista, String valor) {
            String[] itens = lista.split(" ");
            for (int i = 0; i < itens.length; i++) {
                if (itens[i].equals(valor)) return i + 1;
            }
            return -1;
        }
    }

    // ------------------------------------------------------------- REDUCE
    public static class ReduceCorrelacao extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text coluna, Iterable<Text> valores, Context context)
                throws IOException, InterruptedException {
            double n = 0, sx = 0, sy = 0, sxx = 0, syy = 0, sxy = 0;

            // junta as somas parciais que vieram de cada slave
            for (Text t : valores) {
                String[] p = t.toString().split(";");
                n   += Double.parseDouble(p[0]);
                sx  += Double.parseDouble(p[1]);
                sy  += Double.parseDouble(p[2]);
                sxx += Double.parseDouble(p[3]);
                syy += Double.parseDouble(p[4]);
                sxy += Double.parseDouble(p[5]);
            }

            // Pearson: r = (n*sxy - sx*sy) / sqrt((n*sxx - sx^2) * (n*syy - sy^2))
            double r = (n * sxy - sx * sy)
                     / Math.sqrt((n * sxx - sx * sx) * (n * syy - sy * sy));

            String resultado = String.format(java.util.Locale.US, "r = %+.4f\t(n = %d)", r, (long) n);
            context.write(new Text("temp x " + coluna), new Text(resultado));
        }
    }

    // ex.: 1 -> "01_X", 10 -> "10_RH" (para a saida ficar na ordem das colunas)
    static String nomeColuna(int i) {
        return String.format("%02d_%s", i + 1, COLUNAS[i]);
    }

    // --------------------------------------------------------------- MAIN
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "correlacao temperatura");

        job.setJarByClass(CorrelacaoTemperatura.class);
        job.setMapperClass(MapCorrelacao.class);
        job.setReducerClass(ReduceCorrelacao.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
