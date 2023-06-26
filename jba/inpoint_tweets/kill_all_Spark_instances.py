"""
The subprocess module is Python's recommended way to executing shell
commands. It gives us the flexibility to suppress the output of shell
commands or chain inputs and outputs of various commands together, while
still providing a similar experience to os.system() for basic use cases.

In a new filed called list_subprocess.py, write the following code:
"""
import subprocess
import re

# Now let's try to use one of the more advanced features of subprocess.run(),
# namely ignore output to stdout. In the same list_subprocess.py file, change:

# list_files = subprocess.run(["ls", "-l"])
#  To this:

list_spark_instances = subprocess.Popen(["yarn", "application",
                                        "-list"],
                                        stdin=subprocess.PIPE,
                                        stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE,
                                        text=True
                                        )
output, errors = list_spark_instances.communicate(input="Hello from the other side!")

print(f"The Spark instances are: \n{output}")
print(f"The errors: {errors}")

pattern = "(applica\w*_\d*)"

spark_apps = re.findall(pattern,output)
# list_spark_instances = subprocess.Popen(["yarn", "application", "-kill", "spark_app"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
# output, errors = list_spark_instances.communicate(input="Hello from the other side!")
# output, errors = list_spark_instances.communicate()

# print(f"The Spark instances are: \n{output}")
# print(f"The errors: {errors}")
print(spark_apps) # ['application_1643317236156_0028']

# yarn application -kill application_1642894307060_0124
for application in spark_apps:

    list_spark_instances = subprocess.Popen(["yarn", "application",
                                            "-kill", application],
                                            stdin=subprocess.PIPE,
                                            stdout=subprocess.PIPE,
                                            stderr=subprocess.PIPE,
                                            text=True
                                            )
    output, errors = list_spark_instances.communicate(input="Hello from the other side!")
# The standard output of the command now pipes to the special /dev/null device,
# which means the output would not appear on our consoles. Execute the file in 
# your shell to see the following output:


# Pylint giving me "Final new line missing"
# It expects an additional newline character at the end of the file.
# It doesn't really have anything
# to do with the Python code contained in the file.

# pylint(trailing-whitespace)
# Trailing whitespace is any spaces or tabs after the last non-whitespace character
# on the line until the newline.

