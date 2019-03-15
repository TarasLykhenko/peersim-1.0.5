mvn compile

java -cp "target/classes;jep-2.3.0.jar;djep-1.0.0.jar" peersim.Simulator example/config-capstone.txt
java -cp "target/classes;jep-2.3.0.jar;djep-1.0.0.jar" peersim.Simulator example/config-capstonematrix.txt
java -cp "target/classes;jep-2.3.0.jar;djep-1.0.0.jar" peersim.Simulator example/config-cops.txt
java -cp "target/classes;jep-2.3.0.jar;djep-1.0.0.jar" peersim.Simulator example/config-saturn-generic.txt
java -cp "target/classes;jep-2.3.0.jar;djep-1.0.0.jar" peersim.Simulator example/config-occult-no-compression.txt
java -cp "target/classes;jep-2.3.0.jar;djep-1.0.0.jar" peersim.Simulator example/config-occult-compression.txt

python plotter-all.py output/capstone.txt capstone output/capstone-matrix.txt capstone-matrix output/saturn.txt saturn output/cops.txt cops output/occult.txt occult output/occult-compression.txt occult-compression
