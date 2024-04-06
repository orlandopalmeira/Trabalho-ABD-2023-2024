#!/bin/bash

# Verifica se foi fornecida uma mensagem de commit
if [ -z "$1" ]; then
    echo "Por favor, forneça uma mensagem de commit."
    exit 1
fi

# Adiciona todas as mudanças ao staging area
git add .

# Faz o commit com a mensagem fornecida
git commit -m "$1"

# Realiza o push para o repositório remoto
git push
