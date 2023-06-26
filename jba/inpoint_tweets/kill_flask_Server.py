"""
The subprocess module is Python's recommended way to executing shell
commands. It gives us the flexibility to suppress the output of shell
commands or chain inputs and outputs of various commands together, while
still providing a similar experience to os.system() for basic use cases.

In a new filed called list_subprocess.py, write the following code:

https://pythontutor.com/visualize.html#code=import%20re%0A%0A%23%20pattern%20%3D%20%22%28applica%5Cw*_%5Cd*%29%22%0Apattern%20%3D%20'LISTEN%20%28.*%29'%0Aoutput%20%3D%20'tcp%20%20%20%20%20%20%20%200%20%20%20%20%20%200%200.0.0.0%3A5001%20%20%20%20%20%20%20%20%20%20%20%200.0.0.0%3A*%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20LISTEN%20%20%20%20%20%2014370/python%20%20%20%20%20%20%20%20%5Cn'%0A%0A%0Af_output%20%3D%20re.findall%28pattern,output%29%5B0%5D.strip%28%29%0Af_output%3D%20f_output.split%28%22//%22,%200%29%5B0%5D%0Aprocess%20%3D%20re.split%28%22/%22,%20f_output%29%5B0%5D%0Aprint%28process%29&cumulative=false&heapPrimitives=nevernest&mode=edit&origin=opt-frontend.js&py=py3anaconda&rawInputLstJSON=%5B%5D&textReferences=false
"""
import subprocess
import re
from mypass import mypass

# def check_sudo_status():
#     sudo_status = subprocess.Popen('sudo', stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
#     if "usage: sudo" not in sudo_status.stdout.read().decode('utf-8'):
#         print('\nThe command "sudo" dose not exists')
#         return False
#     else:
#         return True

# # def command(cmd):
# #     ans = subprocess.Popen([cmd], stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True).communicate()[0]
# #     return ans


# response = check_sudo_status()

# The easy way to execute sudo command in Python using subprocess.Popen
# run a command as root using sudo from Python:




def mypass():
    mypass = '1234' #or get the password from anywhere
    return mypass
 
echo = subprocess.Popen(['echo',mypass()],
                         stdout=subprocess.PIPE,
                         )

sudo = subprocess.Popen(["sudo", "-S", "netstat", "-tulnp"],
                        stdin=echo.stdout,
                        stdout=subprocess.PIPE,
                        )

end_of_pipe = sudo.stdout
port = str(5000)
# print ("Password ok \n Iptables Chains %s" % end_of_pipe.read())
port = ":"+ port
p2 = subprocess.Popen(["grep", port], stdin = end_of_pipe, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
output, errors = p2.communicate(input ="")
print(f"The Flask instances are: \n{output}")
print(f"The errors: {errors}")

# pattern = "(applica\w*_\d*)"
pattern = 'LISTEN (.*)'

f_output = re.findall(pattern,output)


if f_output:
    f_output = f_output[0].strip()
    process = re.split("/", f_output)[0]
    print(process)

    # sudo kill 902360
    # echo = subprocess.Popen(['echo',mypass()],
    #                          stdout=subprocess.PIPE,
    #                          )

    sudo = subprocess.Popen(["sudo", "-S", "kill", process],
                            stdin=echo.stdout,
                            stdout=subprocess.PIPE,
                            )

# # Now let's try to use one of the more advanced features of subprocess.run(),
# # namely ignore output to stdout. In the same list_subprocess.py file, change:
# sudoPassword=b"1234"
# proc1 = subprocess.Popen(['sudo', '-S'], shell = True, stdout=subprocess.PIPE).communicate(input=b'1234\n')
# # proc2 = subprocess.Popen( sudoPassword,shell = True,stdout=subprocess.PIPE)
# proc3 = subprocess.Popen(['nano', 'test.txt.save'], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True).communicate(input=b'1234\n')

# # Print output and errors
# print(proc3)


# # list_files = subprocess.run(["ls", "-l"])
# #  To this:

# # p1 = subprocess.Popen(["sudo", "netstat", "-tulnp"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

# # run command using Popen and provide password using communicate.
# # password requires byte format.
# # sudo -S brings password request to PIPE
# password=b"1234"
# password = password+b'\n'
# # p1 = subprocess.Popen(["sudo", "-S", "netstat", "-tulnp"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
# # output, errors = p1.communicate(input= password)
# p1={}
# p1['stdout'], p1['stderr'] = subprocess.Popen(["sudo", "-S", "netstat", "-tulnp"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate(input= password)
# # print(p1.stdout)

# p2 = subprocess.Popen(["grep"], stdin = p1['stdout'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
# output, errors = p2.communicate(input ="")
# print(f"The Spark instances are: \n{output}")
# print(f"The errors: {errors}")

# # The standard output of the command now pipes to the special /dev/null device,
# # which means the output would not appear on our consoles. Execute the file in 
# # your shell to see the following output:


# # Pylint giving me "Final new line missing"
# # It expects an additional newline character at the end of the file.
# # It doesn't really have anything
# # to do with the Python code contained in the file.

# # pylint(trailing-whitespace)
# # Trailing whitespace is any spaces or tabs after the last non-whitespace character
# # on the line until the newline.

