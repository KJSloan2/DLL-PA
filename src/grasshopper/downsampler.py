step = z

# Downsampled values
x_down = x[::step]
y_down = y[::step]

idx_down = list(range(0, len(x), step))

print("Original length:", len(x))
print("Downsampled length:", len(x_down))
print("Downsampled x:", x_down)
print("Downsampled y:", y_down)
print("Indexes of kept points:", idx_down)

downsampled = [(i, x[i], y[i]) for i in idx_down]

a = idx_down