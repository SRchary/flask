import definitions


def apply_header_format(workbook, worksheet, dataframe, row_num, column_num):
    """
    This function will format the header of the given sheet from given row_num and column_num.
    :param workbook:
    :param worksheet:
    :param dataframe:
    :param row_num:
    :param column_num:
    :return:
    """
    merge_format = workbook.add_format({
        'bold': 1, 'border': 1,
        'align': 'center', 'valign': 'vcenter',
        'fg_color': '#efefef'
    })
    for col_num, value in enumerate(dataframe.columns.values):
        worksheet.write(row_num, column_num + col_num, value, merge_format)


def apply_border(workbook, worksheet, range_value):
    """
    This function will apply the boarder for given sheet with in given range.
    :param workbook:  Excel Work book object.
    :param worksheet: Excel Work sheet object.
    :param range_value: Range of the row and columns.
    :return:
    """
    cell_format = workbook.add_format({
        'border': 1,
        'align': 'center',
        'valign': 'vcenter'
    })
    cell_format.set_text_wrap()
    worksheet.conditional_format(range_value, {'type': 'no_blanks', 'format': cell_format})
    worksheet.conditional_format(range_value, {'type': 'blanks', 'format': cell_format})
    return worksheet


def merge_cells(workbook, worksheet, cell_range, title):
    """
    This function will merge the given cell_range and set the given title in merged cell.
    :param workbook: workbook object.
    :param worksheet: worksheet object
    :param cell_range: cell range to merge
    :param title: Value for merged cell.
    :return:
    """
    merge_format = workbook.add_format({
        'bold': 1, 'border': 1,
        'align': 'center', 'valign': 'vcenter',
        'fg_color': '#efefef'
    })
    worksheet.merge_range(cell_range, title, merge_format)
    return worksheet


def title_format_cells(workbook, worksheet, cell_range, title):
    """
    This function will apply the font size format to given cell range and set the title value for given cell.
    :param workbook: workbook
    :param worksheet: worksheet
    :param cell_range: filed range(A1, B2)
    :param title: Value for given cell
    :return:
    """
    title_format = workbook.add_format({
        'align': 'center', 'valign': 'vcenter'
    })
    title_format.set_font_size(16)
    worksheet.merge_range(cell_range, title, title_format)
    return worksheet


def format_summary_sheet(workbook, worksheet):
    """
    This function will formet the summery sheet in GRC_4_2_2_2 excel file.
    Will apply border and width of the columns, apply the format for numbers.
    :param workbook: workbook object.
    :param worksheet:worksheet object.
    :return:
    """
    summary_format = workbook.add_format()
    summary_format.set_underline()
    cell_format = workbook.add_format()
    cell_format.set_align('right')
    cell_format.set_align('vcenter')
    cell_left_format = workbook.add_format()
    cell_left_format.set_align('left')
    cell_left_format.set_align('vcenter')
    worksheet.set_column('A:A', 10, cell_left_format)
    worksheet.set_column('B:B', 70, None)
    worksheet.set_column('C:C', 18, cell_format)
    bold_format = workbook.add_format({
        'bold': 1
    })
    bold_format.set_border(0),
    bold_format.set_align("center")
    worksheet.conditional_format("A1:C9", {'type': 'no_blanks', 'format': bold_format})
    red_color_font = workbook.add_format({'bg_color': '#FFFF00',
                                          'fg_color': '#efefef',
                                          'num_format': '#,##0.##',
                                          'font_color': '#181717'})
    red_color_num_font = workbook.add_format({'bg_color': '#FFFF00',
                                          'fg_color': '#efefef',
                                          'num_format': '#,##0',
                                          'font_color': '#181717'})
    decimal_format = workbook.add_format({'num_format': '#,##0.##', })
    num_format = workbook.add_format({'num_format': '#,##0', })
    negative_decimal_format_options = {'type': 'cell',
                               'criteria': 'less than',
                               'value': 0,
                               'format': red_color_font}
    negative_num_format_options = {'type': 'cell',
                                       'criteria': 'less than',
                                       'value': 0,
                                       'format': red_color_num_font}
    positive_decimal_format_options = {'type': 'cell',
                                       'criteria': 'greater than',
                                       'value': 0,
                                       'format': decimal_format}
    positive_num_format_options = {'type': 'cell',
                                   'criteria': 'greater than',
                                   'value': 0,
                                   'format': num_format}

    worksheet.conditional_format("C10", positive_num_format_options)
    worksheet.conditional_format("C10", negative_num_format_options)
    worksheet.conditional_format("C12", positive_num_format_options)
    worksheet.conditional_format("C12", negative_num_format_options)
    worksheet.conditional_format("C14", positive_num_format_options)
    worksheet.conditional_format("C14", negative_num_format_options)
    worksheet.conditional_format("C24", positive_num_format_options)
    worksheet.conditional_format("C24", negative_num_format_options)
    worksheet.conditional_format("C26", positive_num_format_options)
    worksheet.conditional_format("C26", negative_num_format_options)
    worksheet.conditional_format("C16", positive_decimal_format_options)
    worksheet.conditional_format("C16", negative_decimal_format_options)
    worksheet.conditional_format("C18", positive_decimal_format_options)
    worksheet.conditional_format("C18", negative_decimal_format_options)
    worksheet.conditional_format("C20", positive_decimal_format_options)
    worksheet.conditional_format("C20", negative_decimal_format_options)
    worksheet.conditional_format("C22", positive_decimal_format_options)
    worksheet.conditional_format("C22", negative_decimal_format_options)
    return worksheet


def notmapped_mwc_data(workbook, worksheet, dataframe):
    """
    This function will format the header of the given sheet from given row_num and column_num.
    :param workbook:
    :param worksheet:
    :param dataframe:
    :param row_num:
    :param column_num:
    :return:
    """
    merge_format = workbook.add_format({
        'align': 'center', 'valign': 'vcenter',
        'fg_color': '#FFFF00',
        #'font_color': '#FFFF00'
    })
    for row_num, value in enumerate(dataframe["MAT"]):
        if value and value != "NULL" and value[:2] not in definitions.MWC_SELECTED_CODES:
            worksheet.write(row_num+2, 0, value, merge_format)


def format_sheet_1a(workbook, worksheet, dataframes):
    """
    This function will format the line_1a sheet in GRC_4_2_2_2 excel file.
    Will apply border and width of the columns, apply the format for numbers.
    :param workbook: workbook object.
    :param worksheet:worksheet object.
    :param dataframes:dataframe objects as dict.
    :return:
    """
    merge_format = workbook.add_format({
        'bold': 1, 'border': 1,
        'align': 'center', 'valign': 'vcenter',
        'fg_color': '#efefef'
    })
    cell_format = workbook.add_format()
    cell_format.set_align('center')
    cell_format.set_align('vcenter')

    number_format = workbook.add_format()
    number_format.set_align('center')
    number_format.set_align('vcenter')
    number_format.set_num_format(0x03)

    worksheet.set_column('A:A', 20, cell_format)
    worksheet.set_column('B:B', 20, number_format)
    worksheet.set_column('C:C', 10, cell_format)

    worksheet.set_column('D:D', 20, cell_format)
    worksheet.set_column('E:E', 20, number_format)
    worksheet.set_column('F:F', 10, cell_format)

    mat_df = dataframes['mat_df']
    if not mat_df.empty:
        merge_cells(workbook, worksheet, "A1:B1", "Pole Count by MAT Code")
        support_index = mat_df.shape[0] + 3
        total_length = int(mat_df['Count of Poles'].sum())
        worksheet.write("A{}".format(support_index), "Grand Total", merge_format)
        worksheet.write("B{}".format(support_index), "{:,}".format(total_length), merge_format)
        apply_border(workbook, worksheet, "A1:B{}".format(support_index))
        apply_header_format(workbook, worksheet, mat_df, 1, 0)
        notmapped_mwc_data(workbook, worksheet, mat_df)

    mwc_df = dataframes['mwc_df']
    if not mwc_df.empty:
        support_index = mwc_df.shape[0] + 3
        total_length = int(mwc_df['Count of Poles'].sum())
        merge_cells(workbook, worksheet, "D1:E1", "Pole Count by Major Work Category")
        worksheet.write("D{}".format(support_index), "Grand Total", merge_format)
        worksheet.write("E{}".format(support_index), "{:,}".format(total_length), merge_format)
        apply_border(workbook, worksheet, "D1:E{}".format(support_index))
        apply_header_format(workbook, worksheet, mwc_df, 1, 3)
    return worksheet


def format_sheet_1b(workbook, worksheet, dataframes):
    """
    This function will format the line_1b sheet in GRC_4_2_2_2 excel file.
    Will apply border and width of the columns, apply the format for numbers.
    :param workbook: workbook object.
    :param worksheet:worksheet object.
    :param dataframes: line_1b sheet dataframe objects.
    :return:
    """
    merge_format = workbook.add_format({
        'bold': 1, 'border': 1,
        'align': 'center', 'valign': 'vcenter',
        'fg_color': '#efefef'
    })
    cell_format = workbook.add_format()
    cell_format.set_align('center')
    cell_format.set_align('vcenter')

    number_format = workbook.add_format()
    number_format.set_align('center')
    number_format.set_align('vcenter')
    number_format.set_num_format(0x03)

    worksheet.set_column('A:A', 20, cell_format)
    worksheet.set_column('B:B', 20, number_format)
    worksheet.set_column('C:C', 10, cell_format)

    worksheet.set_column('D:D', 20, cell_format)
    worksheet.set_column('E:E', 20, number_format)
    worksheet.set_column('F:F', 18, cell_format)

    merge_cells(workbook, worksheet, "A1:B1", "SUPPORT STRUCTURES")
    merge_cells(workbook, worksheet, "D1:E1", "WOOD POLE COUNT BY AGE")

    support_structure_df = dataframes['support_structure_df']

    support_index = support_structure_df.shape[0] + 3
    total_length = int(support_structure_df['COUNT'].sum())
    worksheet.write("A{}".format(support_index), "Grand Total", merge_format)
    worksheet.write("B{}".format(support_index), "{:,}".format(total_length), merge_format)
    apply_border(workbook, worksheet, "A1:B{}".format(support_index))
    apply_header_format(workbook, worksheet, support_structure_df, 1, 0)

    pole_age_df = dataframes['pole_count_by_age_df']
    pole_age_df['AGE(YEARS)'] = pole_age_df['AGE(YEARS)'].apply(lambda x: x if x else "# N/A")
    support_index = pole_age_df.shape[0] + 3
    total_length = int(pole_age_df['NUMBER OF POLES'].sum())
    worksheet.write("D{}".format(support_index), "Grand Total", merge_format)
    worksheet.write("E{}".format(support_index), "{:,}".format(total_length), merge_format)
    apply_border(workbook, worksheet, "D1:E{}".format(support_index))
    apply_header_format(workbook, worksheet, pole_age_df, 1, 3)
    return worksheet


def format_sheet_1c(workbook, writer, line_1c):
    """
    This function will format the line_1c sheet in GRC_4_2_2_2 excel file.
    Will apply border and width of the columns, apply the format for numbers.
    :param workbook: workbook object.
    :param writer: Excel Writer Object.
    :param line_1c: Line1c Dataframe objects..
    :return:
    """
    merge_format = workbook.add_format({
        'bold': 1, 'border': 1,
        'align': 'center',
        'valign': 'vcenter',
        'fg_color': '#efefef'
    })

    align_format = workbook.add_format({
        'align': 'center',
        'valign': 'vcenter'
    })

    line_1c["pole_class_df"].to_excel(writer, "Line No. 1C", startrow=1, startcol=0, index=False)
    worksheet = writer.sheets['Line No. 1C']

    pole_class_df = line_1c['pole_class_df']
    support_index = pole_class_df.shape[0] + 3
    next_row_start_value = support_index + 4
    total_length = pole_class_df['COUNT'].sum()
    worksheet.write("A{}".format(support_index), "Grand Total", merge_format)
    worksheet.write("B{}".format(support_index), "{:,}".format(int(total_length)), merge_format)
    apply_border(workbook, worksheet, "A2:B{}".format(support_index))
    apply_header_format(workbook, worksheet, pole_class_df, 1, 0)

    pole_species_df = line_1c['pole_species_df']
    support_index = pole_species_df.shape[0] + 3
    next_row_start_value = next_row_start_value if next_row_start_value > (support_index + 3) else (support_index + 3)
    total_length = pole_species_df['COUNT'].sum()
    line_1c["pole_species_df"].to_excel(writer, "Line No. 1C", startrow=1, startcol=4, index=False)
    worksheet = writer.sheets['Line No. 1C']
    worksheet.write("E{}".format(support_index), "Grand Total", merge_format)
    worksheet.write("F{}".format(support_index), "{:,}".format(int(total_length)), merge_format)
    apply_border(workbook, worksheet, "E2:F{}".format(support_index))
    apply_header_format(workbook, worksheet, pole_species_df, 1, 4)

    line_1c["pole_height_df"].to_excel(writer, "Line No. 1C", startrow=next_row_start_value, startcol=0, index=False)
    pole_height_df = line_1c['pole_height_df']
    support_index = pole_height_df.shape[0] + 2 + next_row_start_value
    total_length = pole_height_df['COUNT'].sum()
    worksheet.write("A{}".format(support_index), "Grand Total", merge_format)
    worksheet.write("B{}".format(support_index), "{:,}".format(int(total_length)), merge_format)
    apply_border(workbook, worksheet, "A{}:B{}".format(next_row_start_value + 1, support_index))
    apply_header_format(workbook, worksheet, pole_height_df, next_row_start_value, 0)

    line_1c["pole_treatment_df"].to_excel(writer, "Line No. 1C", startrow=next_row_start_value, startcol=4, index=False)
    pole_treatment_df = line_1c['pole_treatment_df']
    support_index = pole_treatment_df.shape[0] + 2 + next_row_start_value
    total_length = pole_treatment_df['COUNT'].sum()
    worksheet = writer.sheets['Line No. 1C']
    worksheet.write("E{}".format(support_index), "Grand Total", merge_format)
    worksheet.write("F{}".format(support_index), "{:,}".format(int(total_length)), merge_format)
    apply_border(workbook, worksheet, "E{}:F{}".format(next_row_start_value + 1, support_index))
    apply_header_format(workbook, worksheet, pole_treatment_df, next_row_start_value, 4)

    number_format = workbook.add_format()
    number_format.set_align('center')
    number_format.set_align('vcenter')
    number_format.set_num_format(0x03)

    worksheet.set_column('A:A', 20, align_format)
    worksheet.set_column('B:B', 15, number_format)

    worksheet.set_column('E:E', 40, align_format)
    worksheet.set_column('F:F', 15, number_format)
    merge_cells(workbook, worksheet, "A1:B1", "POLE CLASS")
    merge_cells(workbook, worksheet, "E1:F1", "POLE SPECIES")
    merge_cells(workbook, worksheet, "E{}:F{}".format(next_row_start_value, next_row_start_value), "POLE TREATMENT")
    merge_cells(workbook, worksheet, "A{}:B{}".format(next_row_start_value, next_row_start_value), "POLE HEIGHT")
    return worksheet


def format_sheet_line2(workbook, writer, dataframes, year):
    """
    This function will format the line_2 sheet in GRC_4_2_2_2 excel file.
    Will apply border and width of the columns, apply the format for numbers.
    :param workbook: workbook object.
    :param writer:Excel writer object.
    :param dataframes: line_2 dataframe objects as dict.
    :return:
    """
    merge_format = workbook.add_format({
        'bold': 1, 'border': 1,
        'align': 'center', 'valign': 'vcenter',
        'fg_color': '#efefef'
    })

    title_format = workbook.add_format({
        'bold': 1, 'border': 1,
        'align': 'center', 'valign': 'vcenter',
        'fg_color': '#efefef'
    })
    title_format.set_font_size(30)

    cell_format = workbook.add_format()
    cell_format.set_align('center')
    cell_format.set_align('vcenter')

    dataframe_old = dataframes['old_dataframe']
    support_index = dataframe_old.shape[0] + 4
    next_table_rownum = support_index + 5
    dataframe_old.to_excel(writer, "Line No. 2 & 3A", startrow=2, startcol=0, index=False)
    worksheet = writer.sheets['Line No. 2 & 3A']

    if not dataframe_old.empty:
        total_length = dataframe_old['COUNT'].sum()
        dataframe_old.rename(columns={"CONDUCTORCODE": 'Row Labels', "COUNT": 'Sum Of Miles'}, inplace=True)
        worksheet.write("A{}".format(support_index), "Total Result", merge_format)
        worksheet.write("B{}".format(support_index), "{:,}".format(total_length), merge_format)
        apply_border(workbook, worksheet, "A1:B{}".format(support_index))
        apply_header_format(workbook, worksheet, dataframe_old, 2, 0)
        title_format_cells(workbook, worksheet, "A1:B1", "December 31, {}".format(year-1))
        title_format_cells(workbook, worksheet, "A2:B2", "RADIAL")

    dataframe_new = dataframes['new_dataframe']
    if not dataframe_new.empty:
        support_index = dataframe_new.shape[0] + 4
        next_table_rownum = next_table_rownum if next_table_rownum > (support_index + 5) else support_index + 5
        total_length = dataframe_new['COUNT'].sum()
        dataframe_new.rename(columns={"CONDUCTORCODE": 'Row Labels', "COUNT": 'Sum Of Miles'}, inplace=True)
        dataframe_new.to_excel(writer, "Line No. 2 & 3A", startrow=2, startcol=3, index=False)
        worksheet.write("D{}".format(support_index), "Total Result", merge_format)
        worksheet.write("E{}".format(support_index), "{:,}".format(total_length), merge_format)
        apply_border(workbook, worksheet, "D1:E{}".format(support_index))
        apply_header_format(workbook, worksheet, dataframe_new, 2, 3)
        title_format_cells(workbook, worksheet, "D1:E1", "December 31, {}".format(year))
        title_format_cells(workbook, worksheet, "D2:E2", "RADIAL")

    dataframe_old = dataframes['null_old']
    support_index = dataframe_old.shape[0] + 2
    if not dataframe_old.empty:
        total_length = dataframe_old['COUNT'].sum()
        dataframe_old.rename(columns={"CONDUCTORCODE": 'Row Labels', "COUNT": 'Sum Of Miles'}, inplace=True)
        dataframe_old.to_excel(writer, "Line No. 2 & 3A", startrow=next_table_rownum, startcol=0, index=False)
        worksheet.write("A{}".format(support_index + next_table_rownum), "Total Result", merge_format)
        worksheet.write("B{}".format(support_index + next_table_rownum), "{:,}".format(total_length), merge_format)
        apply_border(workbook, worksheet, "A{}:B{}".format(next_table_rownum-1, next_table_rownum + support_index))
        apply_header_format(workbook, worksheet, dataframe_old, next_table_rownum, 0)
        title_format_cells(workbook, worksheet, "A{}:B{}".format(next_table_rownum - 1, next_table_rownum - 1),
                           "December 31, {}".format(year-1))
        title_format_cells(workbook, worksheet, "A{}:B{}".format(next_table_rownum, next_table_rownum),
                           "NULL CIRCUIT")
    dataframe_new = dataframes['null_new']
    if not dataframe_new.empty:
        support_index = dataframe_new.shape[0] + 2
        total_length = dataframe_new['COUNT'].sum()
        dataframe_new.rename(columns={"CONDUCTORCODE": 'Row Labels', "COUNT": 'Sum Of Miles'}, inplace=True)
        dataframe_new.to_excel(writer, "Line No. 2 & 3A", startrow=next_table_rownum, startcol=3, index=False)
        worksheet.write("D{}".format(support_index + next_table_rownum), "Total Result", merge_format)
        worksheet.write("E{}".format(support_index + next_table_rownum), "{:,}".format(total_length), merge_format)
        apply_header_format(workbook, worksheet, dataframe_new, next_table_rownum, 3)
        title_format_cells(workbook, worksheet, "D{}:E{}".format(next_table_rownum - 1, next_table_rownum - 1),
                           "December 31, {}".format(year))

        title_format_cells(workbook, worksheet, "D{}:E{}".format(next_table_rownum, next_table_rownum),
                           "NULL CIRCUIT")
        apply_border(workbook, worksheet, "D{}:E{}".format(next_table_rownum-1, next_table_rownum + support_index))
    worksheet.set_column('A:A', 20, cell_format)
    worksheet.set_column('B:B', 20, cell_format)
    worksheet.set_column('C:C', 10, cell_format)
    worksheet.set_column('D:D', 20, cell_format)
    worksheet.set_column('E:E', 20, cell_format)

    return worksheet


# def format_sheet_line3A(workbook, writer, dataframes, year):
#     """
#     This function will format the line_3A sheet in GRC_4_2_2_2 excel file.
#     Will apply border and width of the columns, apply the format for numbers.
#     :param workbook: workbook object.
#     :param writer: Excelwriter object.
#     :param dataframes: line3A dataframe objects as dict.
#     :param year: end year. Depends on this, will find the previous year and current year.
#     :return:
#
#     """
#     merge_format = workbook.add_format({
#         'bold': 1, 'border': 1,
#         'align': 'center', 'valign': 'vcenter',
#         'fg_color': '#efefef'
#     })
#
#     title_format = workbook.add_format({
#         'bold': 1, 'border': 1,
#         'align': 'center', 'valign': 'vcenter'
#     })
#     title_format.set_font_size(30)
#
#     cell_format = workbook.add_format()
#     cell_format.set_align('center')
#     cell_format.set_align('vcenter')
#
#     dataframe_old = dataframes['old_dataframe']
#     support_index = dataframe_old.shape[0] + 4
#     next_table_rownum = support_index + 5
#
#     dataframe_old.to_excel(writer, "Line No. 3A", startrow=2, startcol=0, index=False)
#     worksheet = writer.sheets['Line No. 3A']
#
#     if not dataframe_old.empty:
#         total_length = dataframe_old['COUNT'].sum()
#         dataframe_old.rename(columns={"CONDUCTORCODE": 'Row Labels', "COUNT": 'Sum Of Miles'}, inplace=True)
#         dataframe_old.to_excel(writer, "Line No. 3A", startrow=2, startcol=0, index=False)
#         worksheet = writer.sheets['Line No. 3A']
#         worksheet.write("A{}".format(support_index), "Total Result", merge_format)
#         worksheet.write("B{}".format(support_index), "{:,}".format(total_length), merge_format)
#         apply_border(workbook, worksheet, "A1:B{}".format(support_index))
#         apply_header_format(workbook, worksheet, dataframe_old, 2, 0)
#         title_format_cells(workbook, worksheet, "A1:B1", "December 31, {}".format(year-1))
#         title_format_cells(workbook, worksheet, "A2:B2", "RADIAL")
#
#     dataframe_new = dataframes['new_dataframe']
#     if not dataframe_new.empty:
#         support_index = dataframe_new.shape[0] + 4
#         next_table_rownum = next_table_rownum if next_table_rownum > (support_index + 5) else support_index + 5
#         total_length = dataframe_new['COUNT'].sum()
#         dataframe_new.rename(columns={"CONDUCTORCODE": 'Row Labels', "COUNT": 'Sum Of Miles'}, inplace=True)
#         dataframe_new.to_excel(writer, "Line No. 3A", startrow=2, startcol=3, index=False)
#         worksheet.write("D{}".format(support_index), "Total Result", merge_format)
#         worksheet.write("E{}".format(support_index), "{:,}".format(total_length), merge_format)
#         apply_border(workbook, worksheet, "D1:E{}".format(support_index))
#         apply_header_format(workbook, worksheet, dataframe_new, 2, 3)
#         title_format_cells(workbook, worksheet, "D1:E1", "December 31, {}".format(year))
#         title_format_cells(workbook, worksheet, "D2:E2", "RADIAL")
#
#     dataframe_old = dataframes['null_old']
#     support_index = dataframe_old.shape[0] + 2
#     if not dataframe_old.empty:
#         total_length = dataframe_old['COUNT'].sum()
#         dataframe_old.rename(columns={"CONDUCTORCODE": 'Row Labels', "COUNT": 'Sum Of Miles'}, inplace=True)
#         dataframe_old.to_excel(writer, "Line No. 3A", startrow=next_table_rownum, startcol=0, index=False)
#         worksheet.write("A{}".format(support_index + next_table_rownum), "Total Result", merge_format)
#         worksheet.write("B{}".format(support_index + next_table_rownum), "{:,}".format(total_length), merge_format)
#         apply_border(workbook, worksheet, "A{}:B{}".format(next_table_rownum-1, next_table_rownum + support_index))
#         apply_header_format(workbook, worksheet, dataframe_old, next_table_rownum, 0)
#         title_format_cells(workbook, worksheet, "A{}:B{}".format(next_table_rownum - 1, next_table_rownum - 1),
#                            "December 31, {}".format(year-1))
#         title_format_cells(workbook, worksheet, "A{}:B{}".format(next_table_rownum, next_table_rownum),
#                            "NULL CIRCUIT")
#
#
#     dataframe_new = dataframes['null_new']
#     if not dataframe_new.empty:
#         support_index = dataframe_new.shape[0] + 2
#         total_length = dataframe_new['COUNT'].sum()
#         dataframe_new.rename(columns={"CONDUCTORCODE": 'Row Labels', "COUNT": 'Sum Of Miles'}, inplace=True)
#         dataframe_new.to_excel(writer, "Line No. 3A", startrow=next_table_rownum, startcol=3, index=False)
#         worksheet.write("D{}".format(support_index + next_table_rownum), "Total Result", merge_format)
#         worksheet.write("E{}".format(support_index + next_table_rownum), "{:,}".format(total_length), merge_format)
#         apply_border(workbook, worksheet, "D{}:E{}".format(next_table_rownum-1, next_table_rownum + support_index))
#         apply_header_format(workbook, worksheet, dataframe_new, next_table_rownum, 3)
#         title_format_cells(workbook, worksheet, "D{}:E{}".format(next_table_rownum - 1, next_table_rownum - 1),
#                            "December 31, {}".format(year))
#
#         title_format_cells(workbook, worksheet, "D{}:E{}".format(next_table_rownum, next_table_rownum),
#                            "NULL CIRCUIT")
#
#     worksheet.set_column('A:A', 20, cell_format)
#     worksheet.set_column('B:B', 20, cell_format)
#     worksheet.set_column('C:C', 10, cell_format)
#     worksheet.set_column('D:D', 20, cell_format)
#     worksheet.set_column('E:E', 20, cell_format)


def format_sheet_line3b(workbook, worksheet, dataframe):
    """
    This function will format the line_3b sheet in GRC_4_2_2_2 excel file.
    Will apply border and width of the columns, apply the format for numbers.
    :param workbook: workbook object.
    :param worksheet:worksheet object.
    :param dataframe:dataframe objects.
    :return:
    """

    merge_format = workbook.add_format({
        'bold': 1, 'border': 1,
        'align': 'center', 'valign': 'vcenter',
        'fg_color': '#efefef'
    })
    cell_format = workbook.add_format()
    cell_format.set_align('center')
    cell_format.set_align('vcenter')

    worksheet.set_column('A:A', 20, cell_format)
    worksheet.set_column('B:B', 20, cell_format)
    worksheet.set_column('C:C', 20, cell_format)

    if dataframe.empty:
        return
    worksheet.set_column('D:D', 25, cell_format)
    worksheet.set_column('E:E', 20, cell_format)
    worksheet.set_column('F:F', 20, cell_format)

    support_index = dataframe.shape[0] + 2
    total_length = dataframe['MILES'].sum()
    worksheet.write("E{}".format(support_index), "Grand Total", merge_format)
    worksheet.write("F{}".format(support_index), "{:,}".format(total_length), merge_format)
    apply_border(workbook, worksheet, "A1:F{}".format(support_index-1))
    apply_header_format(workbook, worksheet, dataframe, 0, 0)
    return worksheet


def format_sheet_line4(workbook, worksheet, dataframes):
    """
    This function will format the line_4 sheet in GRC_4_2_2_2 excel file.
    Will apply border and width of the columns, apply the format for numbers.
    :param workbook: workbook object.
    :param worksheet:worksheet object.
    :param dataframes:dataframes objects as dict.
    :return:
    """
    merge_format = workbook.add_format({
        'bold': 1, 'border': 1,
        'align': 'center', 'valign': 'vcenter',
        'fg_color': '#efefef'
    })
    cell_format = workbook.add_format()
    cell_format.set_align('center')
    cell_format.set_align('vcenter')

    worksheet.set_column('A:A', 15, cell_format)
    worksheet.set_column('B:B', 20, cell_format)
    worksheet.set_column('C:C', 10, cell_format)

    worksheet.set_column('D:D', 15, cell_format)
    worksheet.set_column('E:E', 20, cell_format)
    worksheet.set_column('F:F', 10, cell_format)

    conductor_df = dataframes['conductor_df']
    if not conductor_df.empty:
        support_index = conductor_df.shape[0] + 2
        total_length = conductor_df['LENGTH'].sum()
        worksheet.write("A{}".format(support_index), "Grand Total", merge_format)
        worksheet.write("B{}".format(support_index), "{:,}".format(total_length), merge_format)
        apply_border(workbook, worksheet, "A1:B{}".format(support_index))
        apply_header_format(workbook, worksheet, conductor_df, 0, 0)

    mwc_df = dataframes['mwc_df']
    if not conductor_df.empty:
        support_index = mwc_df.shape[0] + 2
        total_length = mwc_df['LENGTH'].sum()
        worksheet.write("D{}".format(support_index), "Grand Total", merge_format)
        worksheet.write("E{}".format(support_index), "{:,}".format(total_length), merge_format)
        apply_border(workbook, worksheet, "D1:E{}".format(support_index))
        apply_header_format(workbook, worksheet, mwc_df, 0, 3)
    return worksheet


def format_sheet_line5(workbook, worksheet, line_5_df):
    """
    This function will format the line_5 sheet in GRC_4_2_2_2 excel file.
    Will apply border and width of the columns, apply the format for numbers.
    :param workbook: workbook object.
    :param worksheet:worksheet object.
    :param line_5_df:dataframe object.
    :return:
    """
    support_index = line_5_df.shape[0] + 1
    cell_format = workbook.add_format()
    cell_format.set_align('center')
    cell_format.set_align('vcenter')
    worksheet.set_column('A:B', 20, cell_format)
    apply_border(workbook, worksheet, "A1:B{}".format(support_index))
    apply_header_format(workbook, worksheet, line_5_df, 0, 0)
    return worksheet


def format_sheet_line6(workbook, worksheet, line_6_df):
    """
    This function will format the line_6 sheet in GRC_4_2_2_2 excel file.
    Will apply border and width of the columns, apply the format for numbers.
    :param workbook: workbook object.
    :param worksheet:worksheet object.
    :param line_6_df:dataframe object.
    :return:
    """
    if not line_6_df.empty:
        support_index = line_6_df.shape[0] + 1
        cell_format = workbook.add_format()
        cell_format.set_align('center')
        cell_format.set_align('vcenter')
        worksheet.set_column('A:H', 20, cell_format)
        apply_border(workbook, worksheet, "A1:G{}".format(support_index))
        apply_header_format(workbook, worksheet, line_6_df, 0, 0)
    return worksheet
