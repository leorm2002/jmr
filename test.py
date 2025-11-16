import requests
import os
from pathlib import Path

class GutenbergDownloader:
    """Scarica libri da Project Gutenberg per testare MapReduce"""
    
    BOOKS = {
        # Classici grandi (>1MB)
        'shakespeare': {
            'id': '100',
            'title': 'Complete Works of William Shakespeare',
            'size': '~5.5 MB'
        },
        'war_and_peace': {
            'id': '2600',
            'title': 'War and Peace - Tolstoy',
            'size': '~3.2 MB'
        },
        'moby_dick': {
            'id': '2701',
            'title': 'Moby Dick - Melville',
            'size': '~1.2 MB'
        },
        'ulysses': {
            'id': '4300',
            'title': 'Ulysses - Joyce',
            'size': '~1.5 MB'
        },
        'les_miserables': {
            'id': '135',
            'title': 'Les Misérables - Hugo',
            'size': '~3.3 MB'
        },
        'don_quixote': {
            'id': '996',
            'title': 'Don Quixote - Cervantes',
            'size': '~2.2 MB'
        },
        'brothers_karamazov': {
            'id': '28054',
            'title': 'The Brothers Karamazov - Dostoevsky',
            'size': '~1.9 MB'
        },
        'crime_punishment': {
            'id': '2554',
            'title': 'Crime and Punishment - Dostoevsky',
            'size': '~1.2 MB'
        },
        'count_monte_cristo': {
            'id': '1184',
            'title': 'The Count of Monte Cristo - Dumas',
            'size': '~2.6 MB'
        },
        'huckleberry_finn': {
            'id': '76',
            'title': 'Adventures of Huckleberry Finn - Twain',
            'size': '~600 KB'
        },
        
        # Classici medi (500KB-1MB)
        'pride_prejudice': {
            'id': '1342',
            'title': 'Pride and Prejudice - Austen',
            'size': '~700 KB'
        },
        'great_gatsby': {
            'id': '64317',
            'title': 'The Great Gatsby - Fitzgerald',
            'size': '~260 KB'
        },
        'tale_two_cities': {
            'id': '98',
            'title': 'A Tale of Two Cities - Dickens',
            'size': '~780 KB'
        },
        'dracula': {
            'id': '345',
            'title': 'Dracula - Stoker',
            'size': '~880 KB'
        },
        'frankenstein': {
            'id': '84',
            'title': 'Frankenstein - Shelley',
            'size': '~440 KB'
        },
        'jane_eyre': {
            'id': '1260',
            'title': 'Jane Eyre - Brontë',
            'size': '~1.1 MB'
        },
        'wuthering_heights': {
            'id': '768',
            'title': 'Wuthering Heights - Brontë',
            'size': '~720 KB'
        },
        'picture_dorian_gray': {
            'id': '174',
            'title': 'The Picture of Dorian Gray - Wilde',
            'size': '~420 KB'
        },
        'odyssey': {
            'id': '1727',
            'title': 'The Odyssey - Homer',
            'size': '~640 KB'
        },
        'iliad': {
            'id': '6130',
            'title': 'The Iliad - Homer',
            'size': '~800 KB'
        },
        
        # Classici piccoli (<500KB) - ottimi per test rapidi
        'alice': {
            'id': '11',
            'title': 'Alice in Wonderland - Carroll',
            'size': '~170 KB'
        },
        'metamorphosis': {
            'id': '5200',
            'title': 'Metamorphosis - Kafka',
            'size': '~130 KB'
        },
        'heart_darkness': {
            'id': '219',
            'title': 'Heart of Darkness - Conrad',
            'size': '~240 KB'
        },
        'sherlock_holmes': {
            'id': '1661',
            'title': 'Adventures of Sherlock Holmes - Doyle',
            'size': '~580 KB'
        },
        'tom_sawyer': {
            'id': '74',
            'title': 'The Adventures of Tom Sawyer - Twain',
            'size': '~420 KB'
        },
        'jekyll_hyde': {
            'id': '43',
            'title': 'Dr Jekyll and Mr Hyde - Stevenson',
            'size': '~130 KB'
        },
        'time_machine': {
            'id': '35',
            'title': 'The Time Machine - Wells',
            'size': '~190 KB'
        },
        'wizard_oz': {
            'id': '55',
            'title': 'The Wizard of Oz - Baum',
            'size': '~380 KB'
        },
        'jungle_book': {
            'id': '236',
            'title': 'The Jungle Book - Kipling',
            'size': '~310 KB'
        },
        'peter_pan': {
            'id': '16',
            'title': 'Peter Pan - Barrie',
            'size': '~310 KB'
        }
    }
    
    def __init__(self, output_dir='gutenberg_data'):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
    
    def download_book(self, book_key):
        """Scarica un singolo libro"""
        if book_key not in self.BOOKS:
            print(f"Libro '{book_key}' non trovato. Disponibili: {list(self.BOOKS.keys())}")
            return None
        
        book = self.BOOKS[book_key]
        book_id = book['id']
        url = f"https://www.gutenberg.org/files/{book_id}/{book_id}-0.txt"
        
        filename = self.output_dir / f"{book_key}.txt"
        
        print(f"📚 Scaricando: {book['title']} ({book['size']})...")
        print(f"   URL: {url}")
        
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            # Salva il file
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(response.text)
            
            # Statistiche
            size_kb = len(response.text) / 1024
            words = len(response.text.split())
            lines = response.text.count('\n')
            
            print(f"✅ Salvato in: {filename}")
            print(f"   Dimensione: {size_kb:.1f} KB")
            print(f"   Righe: {lines:,}")
            print(f"   Parole: {words:,}")
            print()
            
            return filename
            
        except requests.exceptions.RequestException as e:
            print(f"❌ Errore nel download: {e}")
            return None
    
    def download_all(self):
        """Scarica tutti i libri disponibili"""
        print(f"Scaricando {len(self.BOOKS)} libri in '{self.output_dir}'...\n")
        
        downloaded = []
        for book_key in self.BOOKS:
            result = self.download_book(book_key)
            if result:
                downloaded.append(result)
        
        print(f"\n🎉 Download completato: {len(downloaded)}/{len(self.BOOKS)} libri")
        return downloaded
    
    def list_books(self):
        """Mostra i libri disponibili"""
        print("📖 Libri disponibili per il download:\n")
        for key, book in self.BOOKS.items():
            print(f"  {key:20} - {book['title']} ({book['size']})")
        print()
    
    def create_combined_file(self, book_keys=None):
        """Combina più libri in un unico file per test più grandi"""
        if book_keys is None:
            book_keys = list(self.BOOKS.keys())
        
        combined_file = self.output_dir / "combined_all.txt"
        
        print(f"📦 Creando file combinato con {len(book_keys)} libri...")
        
        with open(combined_file, 'w', encoding='utf-8') as outfile:
            for book_key in book_keys:
                filepath = self.output_dir / f"{book_key}.txt"
                
                if not filepath.exists():
                    print(f"   ⚠️  {book_key}.txt non trovato, lo scarico...")
                    self.download_book(book_key)
                
                if filepath.exists():
                    with open(filepath, 'r', encoding='utf-8') as infile:
                        outfile.write(f"\n\n{'='*80}\n")
                        outfile.write(f"LIBRO: {self.BOOKS[book_key]['title']}\n")
                        outfile.write(f"{'='*80}\n\n")
                        outfile.write(infile.read())
        
        size_mb = combined_file.stat().st_size / (1024 * 1024)
        print(f"✅ File combinato creato: {combined_file}")
        print(f"   Dimensione: {size_mb:.2f} MB")
        
        return combined_file


def main():
    """Esempio di utilizzo"""
    downloader = GutenbergDownloader()
    
    # Scarica i libri più grandi per avere dati sostanziosi
    large_books = ['shakespeare', 'war_and_peace', 'les_miserables', 
                   'don_quixote', 'count_monte_cristo', 'moby_dick']
    
    print(f"\n📥 Scaricando {len(large_books)} libri grandi (tot ~18 MB)...\n")
    for book in GutenbergDownloader.BOOKS.keys():
        print(f"Scaricando {book}...")
        downloader.download_book(book)
    


if __name__ == "__main__":
    main()