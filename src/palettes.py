import seaborn as sns

# PALETTES
vibrant = ['#A7226E', '#EC2049', '#F26B38', '#F7DB47', '#2F9599']

def create_palette(palette: list[str]) -> dict:

    transport_palette = sns.blend_palette([palette[0], '#FFFFFF'], 3).as_hex()
    food_palette = sns.blend_palette([palette[1], '#FFFFFF'], 6).as_hex()
    shopping_palette = sns.blend_palette([palette[2], '#FFFFFF'], 6).as_hex()
    entertainment_palette = [palette[3]]
    holiday_palette = [palette[4]]

    sns.palplot(transport_palette[:-1]+food_palette[:-1]+shopping_palette[:-1]+entertainment_palette+holiday_palette)

    subcategory_palette = {'Total': '#FFFFFF',

                           'Income ': '#FFFFFF',  # with trailing white space for sunburst plot
                           'Income': '#FFFFFF',

                           'Transport ': transport_palette[0],  # with trailing white space for sunburst plot
                           'Transport': transport_palette[0],
                           'Car': transport_palette[1],

                           'Food & Drink ': food_palette[0],  # with trailing white space for sunburst plot
                           'Groceries': food_palette[0],
                           'Snacks': food_palette[1],
                           'Lunch': food_palette[2],
                           'Eating out': food_palette[3],
                           'Alcohol': food_palette[4],

                           'Shopping ': shopping_palette[0],  # with trailing white space for sunburst plot
                           'Shopping': shopping_palette[0],
                           'Clothes': shopping_palette[1],
                           'Electronics': shopping_palette[2],
                           'Personal care': shopping_palette[3],
                           'Gifts': shopping_palette[4],

                           'Entertainment ': entertainment_palette[0],  # with trailing white space for sunburst plot
                           'Entertainment': entertainment_palette[0],

                           'Holidays ': holiday_palette[0],  # with trailing white space for sunburst plot
                           'Holidays': holiday_palette[0],

                           'Bills ': '#FFFFFF',  # with trailing white space for sunburst plot
                           'Bills': '#FFFFFF'}

    return subcategory_palette