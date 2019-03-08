# Used to guarantee to use at least Wx2.8

import numpy as np
import wx

import matplotlib
matplotlib.use('WXAgg')
from matplotlib.backends.backend_wxagg import FigureCanvasWxAgg as FigureCanvas
from matplotlib.backends.backend_wx import NavigationToolbar2Wx
from matplotlib.figure import Figure

def parse(filename):
    result = []
    with open(filename) as fp:
        for cnt, line in enumerate(fp):
            if line.startswith('>>'):
                word_list = line.split()
                result.append(int(word_list[-1]))
    # print("Printing result: ", result)
    return np.array(result)

class MyFileDropTarget(wx.FileDropTarget):
    def __init__(self, window):
        wx.FileDropTarget.__init__(self)
        self.window = window

    def OnDropFiles(self, x, y, filenames):
        points = parse(filenames[0])
        figure = self.window.figure
        axes = figure.get_axes()[0]
        axes.clear()
        #print(type(figure))

        axes.plot(points)
        axes.set_ylim(bottom = 0)
        axes.set_ylim(top = points.max() * 1.2)
        self.window.draw()
        return True
        

class CanvasPanel(wx.Panel):
    def __init__(self, parent):
        wx.Panel.__init__(self, parent)
        self.figure = Figure()
        self.axes = self.figure.add_subplot(111)
        self.canvas = FigureCanvas(self, -1, self.figure)
        self.sizer = wx.BoxSizer(wx.VERTICAL)
        self.sizer.Add(self.canvas, 1, wx.LEFT | wx.TOP | wx.GROW)
        self.SetSizer(self.sizer)
        self.add_toolbar()
        self.Fit()
        

        win_target = self.canvas
        dt = MyFileDropTarget(win_target)
        win_target.SetDropTarget(dt)

    def draw(self):
        self.axes.plot()

    def add_toolbar(self):
        self.toolbar = NavigationToolbar2Wx(self.canvas)
        self.toolbar.Realize()
        if wx.Platform == '__WXMAC__':
            # Mac platform (OSX 10.3, MacPython) does not seem to cope with
            # having a toolbar in a sizer. This work-around gets the buttons
            # back, but at the expense of having the toolbar at the top
            self.SetToolBar(self.toolbar)
        else:
            # On Windows platform, default window size is incorrect, so set
            # toolbar width to figure width.
            tw, th = self.toolbar.GetSize()
            fw, fh = self.canvas.GetSize()
            # By adding toolbar in sizer, we are able to put it at the bottom
            # of the frame - so appearance is closer to GTK version.
            # As noted above, doesn't work for Mac.
            self.toolbar.SetSize(wx.Size(fw, th))
            self.sizer.Add(self.toolbar, 0, wx.LEFT | wx.EXPAND)
        # update the axes menu on the toolbar
        self.toolbar.update()

if __name__ == "__main__":
    app = wx.App()
    frame = wx.Frame(None, title='Plotting Plotter', size=(800, 500))
    panel = CanvasPanel(frame)
    frame.Show()
    app.MainLoop()
