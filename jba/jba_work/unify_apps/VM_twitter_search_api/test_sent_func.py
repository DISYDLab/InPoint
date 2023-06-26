# sent = []
# sent = 0.033

# sent = 0.08
sent = -0.08
# sent = -0.037
def sent_eval(sent_arg):
    if sent_arg > 0.05:
        return 1
    elif sent_arg < -0.05:
        return -1
    else:
        return 0

print(sent_eval(sent))
