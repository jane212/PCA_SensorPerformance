val textFile = sc.textFile("Tingting/pca/error/") //read in data

val counts = textFile.map(line => line.split(",")) //separate line

val lines = counts.map(_.drop(1)).map(_.drop(1)) // get rid of first two columns (name and date column)

import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.feature.StandardScaler

val data = lines.map(s=>Vectors.dense(s.map(_.toDouble))) //make rdd vectors

val scaler = new StandardScaler(withMean = true, withStd = true).fit(data) //scaler model

val scaled = scaler.transform(data) //scaled data

val mat: RowMatrix = new RowMatrix(scaled) //make rowmatrix

val pc: Matrix = mat.computePrincipalComponents(1) //pca

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

val conf = new Configuration()

val fs = FileSystem.get(conf)

val os = fs.create(new Path("Tingting/pca/pc.txt"))

val spc = pc.toArray.mkString(",")

os.write(spc.getBytes)

System.exit(0) //exit spark





