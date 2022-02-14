def printBoard(board):
    for row in range(9):
        for col in range(9):
            print(board[row][col],end='')
        print()

def setBoard():
    board = list()
    Grid_01 = '''200080300
060070084
030500209
000105408
000000000
402706000
301007040
720040060
004010003'''
    rows = Grid_01.split('\n')
    for row in rows:
        column = list()
        for character in row:
            digit = int(character)
            column.append(digit)
        board.append(column)
    #print(board)
    return board

def findEmpty(board):
    for row in range(9):
        for col in range(9):
            if board[row][col] == 0 :
                return row,col
    return None

def isValid(board, num, pos):

    row, col = pos
    # Check if all row elements include this number
    for j in range(9):
        if board[row][j] == num:
            print('Foind in column!')
            return False

    # Check if all column elements include this number
    for i in range(9):
        if board[i][col] == num:
            print('Foind in row!')
            return False

    # Check if the number is already included in the block
    rowBlockStart = 3* (row // 3)
    colBlockStart = 3* (col // 3)

    rowBlockEnd = rowBlockStart + 3
    colBlockEnd = colBlockStart + 3
    for i in range(rowBlockStart, rowBlockEnd):
        for j in range(colBlockStart, colBlockEnd):
            if board[i][j] == num:
                print('Found in block!')
                return False

    return True

def solve(board):
    blank = findEmpty(board)
    print(blank)
    if not blank:
        return True
    else:
        row, col = blank

    for i in range(1,10):
        if isValid(board, i, blank):
            print(i,' IS VALID!')
            board[row][col] = i
            printBoard(board)

            if solve(board):
                return True

            board[row][col] = 0
            print('Erasing now, ehehe')
    return False

board = setBoard()
#printBoard(board)
solve(board)
printBoard(board)
