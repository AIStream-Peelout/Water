def column_renamer(x):
  code_converter_1 = {"00060": "cfs", "00065":"height", "00045": "precip_usgs"}
  split_x = x.split("_")
  if len(split_x) > 1:
    if split_x[1] in code_converter_1 and "cd" not in x:
      return code_converter_1[split_x[1]]
  return x