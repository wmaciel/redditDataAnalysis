__author__ = 'walthermaciel'

from Tkinter import *


def cb_go(author, mail):
    print '/u/' + author
    print 'mail:', mail



def build_img_frame(root):
    img_frame = Frame(root)

    photo = PhotoImage(file='resources/upvote2.gif')
    img_lbl = Label(img_frame, image=photo)
    img_lbl.image = photo # bug workaround. This line keeps the image alive
    img_lbl.pack(side=BOTTOM)

    img_frame.grid(row=0)
    return img_frame


def build_input_frame(root):
    input_frame = Frame(root)

    # \u\ _______________
    u_label = Label(input_frame, text='/u/', pady=5, padx=5)
    u_label.grid(row=0, column=0)
    author_entry = Entry(input_frame)
    author_entry.grid(row=0, column=1, columnspan=3)

    # email _______________
    mail_label = Label(input_frame, text='email', pady=5, padx=5)
    mail_label.grid(row=1, column=0)
    mail_entry = Entry(input_frame)
    mail_entry.grid(row=1, column=1, columnspan=3)

    # Go! button
    go_btn = Button(input_frame, text='Go!', command=lambda: cb_go(author_entry.get(), mail_entry.get()))
    go_btn.grid(row=2, column=3)

    input_frame.grid(row=1)
    return input_frame


def build_gui():
    # Creates main window
    root = Tk()
    root.title('Reddit Analyser')

    img_frame = build_img_frame(root)

    input_frame = build_input_frame(root)

    # Main loop
    root.mainloop()

build_gui()
