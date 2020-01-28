mvn compile

java -Xms1048m -Xmx30000m -cp "target/classes;jep-2.3.0.jar;djep-1.0.0.jar" peersim.Simulator example/config-saturn.txt

#python plotter-all.py output/capstone.txt capstone output/capstone-matrix.txt capstone-matrix output/saturn.txt saturn output/cops.txt cops output/occult.txt occult output/occult-compression.txt occult-compression